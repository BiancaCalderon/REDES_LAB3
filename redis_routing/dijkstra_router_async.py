import uuid, asyncio, heapq
from redis_adapter_async import RedisAdapterAsync

class DijkstraRouterAsync:
    def __init__(self, node_id: str, neighbors: dict, host, port, password):
        """
        neighbors: {canal_vecino: costo}
        """
        self.node_id = node_id
        self.neighbors = neighbors.copy()
        self.adapter = RedisAdapterAsync(node_id, host=host, port=port, password=password)

        # Topología conocida: origin -> {neighbor: cost}
        self.topology = {node_id: neighbors.copy()}
        self.routing_table = {}   # dest -> (next_hop, cost)
        self.seen = set()
        self.update_interval = 15

    async def start(self):
        async def on_message(msg):
            # Ignorar no-JSON
            if not isinstance(msg, dict):
                return

            mid = msg.get("message_id")
            if mid in self.seen:
                return
            self.seen.add(mid)

            t = msg.get("type")
            if t == "message":
                # Si el mensaje es para mí, muestro y retorno
                if msg.get("to") == self.node_id:
                    print(f"[{self.node_id}] 🎯 Recibido de {msg.get('from')}: {msg.get('payload')}")
                    return
                # Si no es para mí, intento reenviar
                await self.forward_message(msg)

            elif t == "topo_update":
                self.handle_topology_update(msg)
                origin = msg.get("from")
                # Reenvía a otros vecinos (excepto el origin para evitar eco)
                for nb in self.neighbors:
                    if nb != origin:
                        await self.adapter.send(nb, msg)

            else:
                return

        await self.adapter.start(on_message)
        # Emite tu topo local y programa refrescos
        await self._emit_topology_update()
        asyncio.create_task(self._periodic_updates())

        # Calcula rutas iniciales
        self.update_routing_table()
        print(f"[{self.node_id}] Nodo DijkstraAsync iniciado. Vecinos: {list(self.neighbors.keys())}")

    def update_routing_table(self):
        # Dijkstra clásico sobre self.topology (grafo dirigido con costos)
        dist = {n: float("inf") for n in self.topology.keys()}
        prev = {n: None for n in self.topology.keys()}
        if self.node_id not in dist:
            dist[self.node_id] = 0.0
        dist[self.node_id] = 0.0

        pq = [(0.0, self.node_id)]
        visited = set()

        while pq:
            d, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)
            for v, w in self.topology.get(u, {}).items():
                nd = d + float(w)
                if nd < dist.get(v, float("inf")):
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        table = {}
        for dest in list(dist.keys()):
            if dest == self.node_id or dist[dest] == float("inf"):
                continue
            # reconstrucción de camino
            path = []
            cur = dest
            while cur is not None:
                path.append(cur)
                cur = prev.get(cur)
            path.reverse()
            if len(path) > 1:
                table[dest] = (path[1], dist[dest])

        self.routing_table = table
        print(f"[{self.node_id}] Tabla de ruteo: {self.routing_table}")

    async def forward_message(self, msg):
        dest = msg["to"]

        # Fallback: si es vecino directo, manda directo aunque aún no haya SPF
        if dest in self.neighbors:
            msg["ttl"] = msg.get("ttl", 10) - 1
            if msg["ttl"] <= 0:
                return
            await self.adapter.send(dest, msg)
            print(f"[{self.node_id}] 📡 (vecino) → {dest}")
            return

        if dest not in self.routing_table:
            print(f"[{self.node_id}] No hay ruta a {dest}")
            return
        next_hop, _ = self.routing_table[dest]
        msg["ttl"] = msg.get("ttl", 10) - 1
        if msg["ttl"] <= 0:
            print(f"[{self.node_id}] TTL agotado para {dest}")
            return
        await self.adapter.send(next_hop, msg)
        print(f"[{self.node_id}] Reenviado hacia {dest} via {next_hop}")

    def handle_topology_update(self, msg):
        topo_info = msg.get("headers", {}).get("topo", {})
        origin = msg.get("from")
        # Actualiza la topología para ese origin
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
