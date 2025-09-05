import uuid, asyncio, heapq, time
from redis_adapter_async import RedisAdapterAsync

class DijkstraRouterAsync:
    def __init__(self, node_id: str, neighbors: dict, host, port, password):
        self.node_id = node_id
        self.neighbors = neighbors  # {nb: cost}
        self.adapter = RedisAdapterAsync(node_id, host=host, port=port, password=password)
        self.topology = {node_id: neighbors.copy()}
        self.routing_table = {}
        self.seen = set()
        self.update_interval = 15

    async def start(self):
        async def on_message(msg):
            mid = msg.get("message_id")
            if mid in self.seen:
                return
            self.seen.add(mid)

            t = msg.get("type")
            if t == "message":
                if msg["to"] == self.node_id:
                    print(f"[{self.node_id}] Recibido de {msg['from']}: {msg['payload']}")
                else:
                    await self.forward_message(msg)
            elif t == "topo_update":
                self.handle_topology_update(msg)
                origin = msg["from"]
                # reenvía a otros
                for nb in self.neighbors:
                    if nb != origin:
                        await self.adapter.send(nb, msg)

        await self.adapter.start(on_message)
        await self._emit_topology_update()
        asyncio.create_task(self._periodic_updates())
        self.update_routing_table()
        print(f"[{self.node_id}] Nodo DijkstraAsync iniciado. Vecinos: {list(self.neighbors.keys())}")

    def update_routing_table(self):
        dist = {n: float("inf") for n in self.topology}
        prev = {n: None for n in self.topology}
        dist[self.node_id] = 0
        pq = [(0, self.node_id)]

        while pq:
            d, u = heapq.heappop(pq)
            for v, w in self.topology.get(u, {}).items():
                nd = d + w
                if nd < dist.get(v, float("inf")):
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        self.routing_table = {}
        for dest in dist:
            if dest != self.node_id and dist[dest] < float("inf"):
                path = []
                cur = dest
                while cur is not None:
                    path.append(cur)
                    cur = prev[cur]
                path.reverse()
                if len(path) > 1:
                    self.routing_table[dest] = (path[1], dist[dest])
        print(f"[{self.node_id}] Tabla de ruteo: {self.routing_table}")

    async def forward_message(self, msg):
        dest = msg["to"]
        if dest not in self.routing_table:
            print(f"[{self.node_id}] No hay ruta a {dest}")
            return
        next_hop, _ = self.routing_table[dest]
        msg["ttl"] -= 1
        if msg["ttl"] <= 0:
            print(f"[{self.node_id}] TTL agotado para {dest}")
            return
        await self.adapter.send(next_hop, msg)
        print(f"[{self.node_id}] Reenviado hacia {dest} via {next_hop}")

    def handle_topology_update(self, msg):
        topo_info = msg.get("headers", {}).get("topo", {})
        origin = msg["from"]
        self.topology[origin] = topo_info
        self.update_routing_table()

    async def send_message(self, dest, payload):
        msg = {
            "proto": "dijkstra",
            "type": "message",
            "from": self.node_id,
            "to": dest,
            "ttl": 10,
            "headers": {},
            "payload": payload,
            "message_id": str(uuid.uuid4())[:8]
        }
        self.seen.add(msg["message_id"])
        await self.forward_message(msg)

    async def _emit_topology_update(self):
        msg = {
            "proto": "dijkstra",
            "type": "topo_update",
            "from": self.node_id,
            "to": "*",
            "ttl": 8,
            "headers": {"topo": self.neighbors},
            "payload": "",
            "message_id": str(uuid.uuid4())[:8]
        }
        self.seen.add(msg["message_id"])
        for nb in self.neighbors:
            await self.adapter.send(nb, msg)

    async def _periodic_updates(self):
        while True:
            await asyncio.sleep(self.update_interval)
            await self._emit_topology_update()
