import asyncio, uuid, time, math, heapq
from redis_adapter_async import RedisAdapterAsync

HELLO_INTERVAL = 3                  # cada 3s
NEIGHBOR_TTL   = 30                 # 10 hellos -> 30s
REMOTE_TTL     = 15                 # edges aprendidos por flooding
RECOMPUTE_DEBOUNCE = 0.5            # evitar recomputar demasiadas veces

class NodeLSAsync:
    def __init__(self, node_id: str, neighbors: dict, host, port, password):
        """
        node_id   : 'sec30.grupo8.nodo8'
        neighbors : { 'sec30.grupo7.nodo7': 7, ... }
        """
        self.node_id = node_id
        self.neighbors = neighbors.copy()
        self.adapter = RedisAdapterAsync(node_id, host=host, port=port, password=password)

        # Tabla interna: nodo -> { vecino: {weight, time} }
        self.topology = {node_id: {nb: {"weight": w, "time": NEIGHBOR_TTL} for nb, w in self.neighbors.items()}}
        # Para edges simétricos (opcional, ayuda a conectar desde ambos lados)
        for nb, w in self.neighbors.items():
            self.topology.setdefault(nb, {})
            self.topology[nb][node_id] = {"weight": w, "time": NEIGHBOR_TTL}

        self.seen = set()            # ids de mensajes para evitar loops
        self.routing_table = {}      # dest -> (next_hop, cost)
        self._pending_recompute = False

    # ------------------ arranque ------------------
    async def start(self):
        async def on_message(msg):
            if not isinstance(msg, dict):
                return

            mid = msg.get("message_id")
            if mid in self.seen:
                return
            self.seen.add(mid)

            mtype = msg.get("type")
            if mtype == "hello":
                self._handle_hello(msg)
                return

            if mtype == "message":
                changed = self._handle_message(msg)
                # re-flood SOLO si aprendimos algo nuevo o cambió el peso
                if changed:
                    await self._flood(msg, exclude=msg.get("from"))
                await self._schedule_recompute()
                return

        await self.adapter.start(on_message)
        # Lo primero: anunciar a vecinos mis enlaces (como 'message') y empezar hellos/timers
        await self._announce_my_links()
        asyncio.create_task(self._hello_loop())
        asyncio.create_task(self._timer_loop())
        print(f"[{self.node_id}] Nodo LSR (spec) iniciado. Vecinos: {list(self.neighbors.keys())}")

    # ------------------ handlers ------------------
    def _handle_hello(self, msg: dict):
        origin = msg.get("from")
        hops   = float(msg.get("hops", 1))
        # resetea timer del enlace directo origin<->self
        self.topology.setdefault(self.node_id, {})
        self.topology[self.node_id].setdefault(origin, {"weight": hops, "time": 0})
        self.topology[self.node_id][origin]["time"] = NEIGHBOR_TTL
        self.topology[self.node_id][origin]["weight"] = hops

        # (opcional) mantén simétrico
        self.topology.setdefault(origin, {})
        self.topology[origin][self.node_id] = {"weight": hops, "time": NEIGHBOR_TTL}

    def _handle_message(self, msg: dict) -> bool:
        """
        Devuelve True si la topología **cambió** (nuevo edge o peso distinto).
        Mensaje formato:
          { type: "message", from: "A", to: "B", hops: peso }
        """
        u = msg.get("from")
        v = msg.get("to")
        w = float(msg.get("hops", 1))

        self.topology.setdefault(u, {})
        cur = self.topology[u].get(v)

        changed = False
        if cur is None or cur["weight"] != w:
            self.topology[u][v] = {"weight": w, "time": REMOTE_TTL}
            changed = True
        else:
            # refresca TTL para edges remotos
            cur["time"] = REMOTE_TTL

        return changed

    # ------------------ envío ------------------
    async def _hello_loop(self):
        while True:
            await asyncio.sleep(HELLO_INTERVAL)
            for nb, w in self.neighbors.items():
                hello = {
                    "type": "hello",
                    "from": self.node_id,
                    "to": nb,
                    "hops": w,
                    "message_id": str(uuid.uuid4())[:8]
                }
                await self.adapter.send(nb, hello)

    async def _announce_my_links(self):
        # publica mis enlaces directos como 'message' a TODOS mis vecinos (para que inicie el flooding)
        for nb, w in self.neighbors.items():
            msg = {
                "type": "message",
                "from": self.node_id,
                "to": nb,
                "hops": w,
                "message_id": str(uuid.uuid4())[:8]
            }
            self.seen.add(msg["message_id"])
            await self._flood(msg)

    async def _flood(self, msg: dict, exclude: str = None):
        for nb in self.neighbors.keys():
            if nb == exclude:
                continue
            await self.adapter.send(nb, msg)

    # ------------------ timers / expiración ------------------
    async def _timer_loop(self):
        while True:
            await asyncio.sleep(HELLO_INTERVAL)
            dirty = False
            # decrementa TTLs
            for u in list(self.topology.keys()):
                for v in list(self.topology[u].keys()):
                    self.topology[u][v]["time"] -= HELLO_INTERVAL
                    if self.topology[u][v]["time"] <= 0:
                        # borra enlace expirado
                        del self.topology[u][v]
                        dirty = True
                if not self.topology[u]:
                    del self.topology[u]
                    dirty = True
            if dirty:
                await self._schedule_recompute()

    async def _schedule_recompute(self):
        if self._pending_recompute:
            return
        self._pending_recompute = True
        await asyncio.sleep(RECOMPUTE_DEBOUNCE)
        self._recompute_routes()
        self._pending_recompute = False

    # ------------------ SPF (Dijkstra) ------------------
    def _graph_now(self):
        """Convierte tabla interna -> grafo simple {u:{v:weight}} ignorando enlaces expirados."""
        g = {}
        for u, adj in self.topology.items():
            for v, meta in adj.items():
                if meta["time"] > 0:
                    g.setdefault(u, {})[v] = float(meta["weight"])
        return g

    def _recompute_routes(self):
        g = self._graph_now()
        if self.node_id not in g and self.node_id not in {k for a in g.values() for k in a.keys()}:
            self.routing_table = {}
            return

        # nodes
        nodes = set(g.keys())
        for u in g:
            nodes.update(g[u].keys())
        nodes = list(nodes)
        if self.node_id not in nodes:
            nodes.append(self.node_id)

        dist = {n: math.inf for n in nodes}
        prev = {n: None for n in nodes}
        dist[self.node_id] = 0.0

        pq = [(0.0, self.node_id)]
        seen = set()
        while pq:
            d, u = heapq.heappop(pq)
            if u in seen: continue
            seen.add(u)
            for v, w in g.get(u, {}).items():
                nd = d + w
                if nd < dist.get(v, math.inf):
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        rt = {}
        for dst in nodes:
            if dst == self.node_id or dist.get(dst, math.inf) == math.inf:
                continue
            # reconstruir camino
            path = []
            cur = dst
            while cur is not None:
                path.append(cur)
                cur = prev.get(cur)
            path.reverse()
            if len(path) > 1:
                rt[dst] = (path[1], dist[dst])

        self.routing_table = rt
        # imprime pequeño resumen
        print(f"[{self.node_id}] rutas: {self.routing_table}")
