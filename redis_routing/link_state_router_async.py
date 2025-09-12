import uuid, asyncio, heapq, time, math, json
from redis_adapter_async import RedisAdapterAsync

class LinkStateRouterAsync:
    def __init__(self, node_id: str, neighbors: dict, host, port, password, print_updates=False):
        self.node_id = node_id
        self.neighbors = neighbors                   # {canal_vecino: costo}
        self.adapter = RedisAdapterAsync(node_id, host=host, port=port, password=password)
        self.lsdb = {}                               # origin -> {seq, links, last_update}
        self.routing_table = {}                      # dest -> (next_hop, cost)
        self.seen = set()
        self.lsp_seq = 0
        self.refresh_interval = 20                   # segundos
        self.expire_time = 180                       # caducidad de LSPs (s)
        self.print_updates = print_updates
        self._rt_last_hash = None                    # para imprimir solo si cambia

    async def start(self):
        async def on_message(msg):
            if not isinstance(msg, dict):
                # ignorar no-JSON
                return

            mid = msg.get("message_id")
            if mid in self.seen:
                return
            self.seen.add(mid)

            t = msg.get("type")

            if t == "lsp":
                lsp = msg.get("headers", {}).get("lsp", {})
                updated = self._accept_lsp(lsp)
                if updated:
                    self._recompute()
                # Flood a vecinos (evitar devolver al emisor)
                origin_sender = msg.get("from")
                for nb in self.neighbors:
                    if nb != origin_sender:
                        await self.adapter.send(nb, msg)

            elif t == "message":
                if msg.get("to") == self.node_id:
                    print(f"[{self.node_id}] 🎯 Recibido de {msg.get('from')}: {msg.get('payload')}")
                    return
                await self.forward_message(msg)

            else:
                return

        await self.adapter.start(on_message)

        # Instalar MI LSP en LSDB, recomputar y floodear a vecinos
        await self._install_own_lsp_and_flood()

        # Refrescos periódicos
        asyncio.create_task(self._periodic_lsp())

        print(f"[{self.node_id}] Nodo LSR Async iniciado. Vecinos: {list(self.neighbors.keys())}")

    # ----------------- LSDB / LSP -----------------

    def _accept_lsp(self, lsp: dict) -> bool:
        origin = lsp.get("origin")
        if origin is None:
            return False
        seq = lsp.get("seq", 0)
        if origin not in self.lsdb or seq > self.lsdb[origin]["seq"]:
            self.lsdb[origin] = {
                "seq": seq,
                "links": lsp.get("links", {}),
                "last_update": time.time()
            }
            return True
        return False

    def _graph(self) -> dict:
        now = time.time()
        g = {}
        for origin, info in list(self.lsdb.items()):
            if now - info["last_update"] > self.expire_time:
                self.lsdb.pop(origin)
                continue
            g[origin] = info["links"]
        return g

    async def _install_own_lsp_and_flood(self):
        self.lsp_seq += 1
        hdr = {
            "lsp": {
                "origin": self.node_id,
                "seq": self.lsp_seq,
                "links": self.neighbors,
                "age": time.time()
            }
        }
        msg = {
            "proto": "lsr",
            "type": "lsp",
            "from": self.node_id,
            "to": "*",
            "ttl": 10,
            "headers": hdr,
            "payload": "",
            "message_id": str(uuid.uuid4())[:8]
        }

        # Instalar mi propio LSP en LSDB y recomputar rutas
        self.lsdb[self.node_id] = {
            "seq": self.lsp_seq,
            "links": self.neighbors,
            "last_update": time.time()
        }
        self._recompute()

        # Flood a todos los vecinos
        self.seen.add(msg["message_id"])
        for nb in self.neighbors:
            await self.adapter.send(nb, msg)

    async def _periodic_lsp(self):
        while True:
            await asyncio.sleep(self.refresh_interval)
            await self._install_own_lsp_and_flood()

    # ----------------- SPF / Tabla de ruteo -----------------

    def _recompute(self):
        g = self._graph()

        # Incluir todos los nodos (orígenes y destinos)
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
        visited = set()
        while pq:
            d, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)
            for v, w in g.get(u, {}).items():
                nd = d + float(w)
                if nd < dist.get(v, math.inf):
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        new_rt = {}
        for dest in nodes:
            if dest == self.node_id or dist.get(dest, math.inf) == math.inf:
                continue
            # reconstruir path
            path = []
            cur = dest
            while cur is not None:
                path.append(cur)
                cur = prev.get(cur)
            path.reverse()
            if len(path) > 1:
                new_rt[dest] = (path[1], dist[dest])

        # Solo imprime si cambió y print_updates=True
        rt_hash = hash(json.dumps(sorted(new_rt.items())))
        changed = (rt_hash != self._rt_last_hash)
        self.routing_table = new_rt
        if self.print_updates and changed:
            print(f"[{self.node_id}] 🗺️ Tabla rutas: {self.routing_table}")
        self._rt_last_hash = rt_hash

    # ----------------- Forwarding -----------------

    async def forward_message(self, msg: dict):
        dest = msg["to"]

        # Fallback: si es vecino directo, envía directo aunque no haya SPF
        if dest in self.neighbors:
            msg["ttl"] = msg.get("ttl", 16) - 1
            if msg["ttl"] <= 0:
                return
            await self.adapter.send(dest, msg)
            print(f"[{self.node_id}] 📡 (vecino) → {dest}")
            return

        if dest not in self.routing_table:
            print(f"[{self.node_id}] ⚠️ Sin ruta hacia {dest}")
            return

        next_hop, _ = self.routing_table[dest]
        msg["ttl"] = msg.get("ttl", 16) - 1
        if msg["ttl"] <= 0:
            return
        await self.adapter.send(next_hop, msg)
        print(f"[{self.node_id}] 📦 {dest} via {next_hop}")

    async def send_message(self, dest: str, payload: str):
        msg = {
            "proto": "lsr",
            "type": "message",
            "from": self.node_id,
            "to": dest,
            "ttl": 16,
            "headers": {},
            "payload": payload,
            "message_id": str(uuid.uuid4())[:8]
        }
        self.seen.add(msg["message_id"])
        await self.forward_message(msg)
