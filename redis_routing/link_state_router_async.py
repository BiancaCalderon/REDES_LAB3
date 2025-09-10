import uuid, asyncio, heapq, time
from redis_adapter_async import RedisAdapterAsync

class LinkStateRouterAsync:
    def __init__(self, node_id: str, neighbors: dict, host, port, password):
        self.node_id = node_id
        self.neighbors = neighbors
        self.adapter = RedisAdapterAsync(node_id, host=host, port=port, password=password)
        self.lsdb = {}
        self.routing_table = {}
        self.seen = set()
        self.lsp_seq = 0
        self.refresh_interval = 20
        self.expire_time = 180

    async def start(self):
        async def on_message(msg):
            mid = msg.get("message_id")
            if mid in self.seen:
                return
            self.seen.add(mid)

            t = msg.get("type")
            if t == "lsp":
                lsp = msg.get("headers", {}).get("lsp", {})
                origin = lsp.get("origin")
                updated = self._accept_lsp(lsp)
                if updated:
                    self._recompute()
                # flood a vecinos
                for nb in self.neighbors:
                    if nb != msg["from"]:
                        await self.adapter.send(nb, msg)
            elif t == "message":
                await self.forward_message(msg)

        await self.adapter.start(on_message)
        await self._emit_lsp()
        asyncio.create_task(self._periodic_lsp())
        print(f"[{self.node_id}] Nodo LSR Async iniciado. Vecinos: {list(self.neighbors.keys())}")

    def _accept_lsp(self, lsp):
        origin = lsp["origin"]
        seq = lsp["seq"]
        if origin not in self.lsdb or seq > self.lsdb[origin]["seq"]:
            self.lsdb[origin] = {"seq": seq, "links": lsp["links"], "last_update": time.time()}
            return True
        return False

    def _graph(self):
        now = time.time()
        g = {}
        for origin, info in list(self.lsdb.items()):
            if now - info["last_update"] > self.expire_time:
                self.lsdb.pop(origin)
                continue
            g[origin] = info["links"]
        return g

    def _recompute(self):
        g = self._graph()
        nodes = set(g.keys())
        for u in g:
            nodes.update(g[u].keys())
        nodes = list(nodes)

        import math, heapq
        dist = {n: math.inf for n in nodes}
        prev = {n: None for n in nodes}
        if self.node_id not in dist:
            dist[self.node_id] = 0  # por seguridad
            nodes.append(self.node_id)
        dist[self.node_id] = 0

        pq = [(0, self.node_id)]
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

        self.routing_table = {}
        for dest in nodes:
            if dest == self.node_id or dist.get(dest, math.inf) == math.inf:
                continue
            # reconstruir path
            path, cur = [], dest
            while cur is not None:
                path.append(cur); cur = prev.get(cur)
            path.reverse()
            if len(path) > 1:
                self.routing_table[dest] = (path[1], dist[dest])
        print(f"[{self.node_id}] 🗺️ Tabla rutas: {self.routing_table}")
    
    async def _emit_lsp(self):
        self.lsp_seq += 1
        hdr = {"lsp": {"origin": self.node_id, "seq": self.lsp_seq, "links": self.neighbors, "age": time.time()}}
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
        self.seen.add(msg["message_id"])
        for nb in self.neighbors:
            await self.adapter.send(nb, msg)

    async def _periodic_lsp(self):
        while True:
            await asyncio.sleep(self.refresh_interval)
            await self._emit_lsp()

    async def forward_message(self, msg):
        dest = msg["to"]
        if dest in self.neighbors:
            msg["ttl"] -= 1
            if msg["ttl"] <= 0: 
                return
            await self.adapter.send(dest, msg)
            print(f"[{self.node_id}] 📡 (vecino) → {dest}")
            return

        if dest not in self.routing_table:
            print(f"[{self.node_id}] ⚠️ Sin ruta hacia {dest}")
            return
        next_hop, _ = self.routing_table[dest]
        msg["ttl"] -= 1
        if msg["ttl"] <= 0: 
            return
        await self.adapter.send(next_hop, msg)
        print(f"[{self.node_id}] 📦 {dest} via {next_hop}")

    async def send_message(self, dest, payload):
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
