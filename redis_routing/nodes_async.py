import asyncio, uuid, time, math, heapq
from redis_adapter_async import RedisAdapterAsync

class NodeAsync:
    def __init__(self, node_id, neighbors, host, port, password):
        """
        neighbors: { vecino: peso }
        """
        self.node_id = node_id
        self.neighbors = neighbors
        self.adapter = RedisAdapterAsync(node_id, host=host, port=port, password=password)

        # topología interna: nodo -> { vecino: {weight, time} }
        self.topology = {node_id: {nb: {"weight": w, "time": 30} for nb,w in neighbors.items()}}
        self.seen = set()
        self.routing_table = {}

    async def start(self):
        async def on_message(msg):
            if not isinstance(msg, dict):
                return
            mid = msg.get("message_id")
            if mid in self.seen: return
            self.seen.add(mid)

            t = msg.get("type")
            if t == "hello":
                self._handle_hello(msg)
            elif t == "message":
                updated = self._handle_message(msg)
                if updated:
                    await self._flood(msg, exclude=msg["from"])

        await self.adapter.start(on_message)

        # Arranca timers
        asyncio.create_task(self._hello_loop())
        asyncio.create_task(self._timer_loop())
        print(f"[{self.node_id}] iniciado. Vecinos: {list(self.neighbors.keys())}")

    def _handle_hello(self, msg):
        origin = msg["from"]
        hops = msg["hops"]
        # resetear timer
        if origin not in self.topology.get(self.node_id, {}):
            self.topology[self.node_id][origin] = {"weight": hops, "time": 30}
        else:
            self.topology[self.node_id][origin]["time"] = 30

    def _handle_message(self, msg):
        origin, dest, hops = msg["from"], msg["to"], msg["hops"]
        # insertar en tabla interna
        changed = False
        if origin not in self.topology:
            self.topology[origin] = {}
        if dest not in self.topology[origin] or self.topology[origin][dest]["weight"] != hops:
            self.topology[origin][dest] = {"weight": hops, "time": 30}
            changed = True
        return changed

    async def _flood(self, msg, exclude=None):
        for nb in self.neighbors:
            if nb != exclude:
                await self.adapter.send(nb, msg)

    async def _hello_loop(self):
        while True:
            await asyncio.sleep(3)
            for nb, w in self.neighbors.items():
                hello = {
                    "type": "hello",
                    "from": self.node_id,
                    "to": nb,
                    "hops": w,
                    "message_id": str(uuid.uuid4())[:8]
                }
                await self.adapter.send(nb, hello)

                msg = {
                    "type": "message",
                    "from": self.node_id,
                    "to": nb,
                    "hops": w,
                    "message_id": str(uuid.uuid4())[:8]
                }
                await self._flood(msg)

    async def _timer_loop(self):
        while True:
            await asyncio.sleep(3)
            # reducir timers
            for u in list(self.topology.keys()):
                for v in list(self.topology[u].keys()):
                    self.topology[u][v]["time"] -= 3
                    if self.topology[u][v]["time"] <= 0:
                        print(f"[{self.node_id}] 🔻 Enlace {u}—{v} expiró")
                        del self.topology[u][v]
            self._recompute_routes()

    def _recompute_routes(self):
        g = {u: {v: d["weight"] for v,d in adj.items()} for u,adj in self.topology.items()}
        dist = {n: math.inf for n in g}
        prev = {n: None for n in g}
        if self.node_id not in g: return
        dist[self.node_id] = 0
        pq = [(0, self.node_id)]
        while pq:
            d,u = heapq.heappop(pq)
            for v,w in g.get(u, {}).items():
                nd = d+w
                if nd < dist.get(v, math.inf):
                    dist[v]=nd; prev[v]=u
                    heapq.heappush(pq,(nd,v))
        rt = {}
        for dest in dist:
            if dest==self.node_id or dist[dest]==math.inf: continue
            path=[]; cur=dest
            while cur: path.append(cur); cur=prev.get(cur)
            path.reverse()
            if len(path)>1: rt[dest]=(path[1], dist[dest])
        self.routing_table = rt
        print(f"[{self.node_id}] 🗺️ Tabla rutas: {self.routing_table}")
