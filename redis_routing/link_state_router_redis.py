# redis_routing/link_state_router_redis.py
import uuid, time, heapq
from redis_adapter import RedisAdapter

class LinkStateRouterRedis:
    def __init__(self, node_id: str, neighbors: dict):
  
        self.node_id = node_id
        self.neighbors = neighbors
        self.lsdb = {} 
        self.routing_table = {}
        self.seq = 0
        self.seen_messages = set()
        self.adapter = RedisAdapter(node_id)

    def start(self):
        def on_message(msg):
            mid = msg.get("message_id")
            if mid in self.seen_messages:
                return
            self.seen_messages.add(mid)

            if msg["type"] == "message":
                if msg["to"] == self.node_id:
                    print(f"[{self.node_id}] Recibido: {msg['payload']}")
                else:
                    self.forward_message(msg)
            elif msg["type"] == "lsp":
                self.handle_lsp(msg)

        self.adapter.start(on_message)
        self.advertise_lsp()
        print(f"[{self.node_id}] Nodo LSR iniciado. Vecinos: {list(self.neighbors.keys())}")

    def advertise_lsp(self):
        self.seq += 1
        lsp = {
            "proto": "lsr",
            "type": "lsp",
            "from": self.node_id,
            "to": "*",
            "ttl": 10,
            "headers": {"links": self.neighbors, "seq": self.seq},
            "payload": "",
            "message_id": str(uuid.uuid4())[:8]
        }
        self.lsdb[self.node_id] = {"seq": self.seq, "links": self.neighbors, "time": time.time()}
        for nb in self.neighbors:
            self.adapter.send(nb, lsp)
        self.recompute_routes()

    def handle_lsp(self, msg):
        origin = msg["from"]
        seq = msg["headers"]["seq"]
        links = msg["headers"]["links"]
        if origin not in self.lsdb or seq > self.lsdb[origin]["seq"]:
            self.lsdb[origin] = {"seq": seq, "links": links, "time": time.time()}
            for nb in self.neighbors:
                if nb != msg["from"]:
                    self.adapter.send(nb, msg)
            self.recompute_routes()

    def recompute_routes(self):

        graph = {o: info["links"] for o, info in self.lsdb.items()}
        dist = {n: float("inf") for n in graph}
        prev = {n: None for n in graph}
        dist[self.node_id] = 0
        pq = [(0, self.node_id)]

        while pq:
            d, u = heapq.heappop(pq)
            for v, w in graph.get(u, {}).items():
                nd = d + w
                if nd < dist.get(v, float("inf")):
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        self.routing_table = {}
        for dest in dist:
            if dest == self.node_id or dist[dest] == float("inf"):
                continue
            path = []
            cur = dest
            while cur:
                path.append(cur)
                cur = prev[cur]
            path.reverse()
            if len(path) > 1:
                self.routing_table[dest] = (path[1], dist[dest])
        print(f"[{self.node_id}] Rutas: {self.routing_table}")

    def forward_message(self, msg):
        dest = msg["to"]
        if dest not in self.routing_table:
            print(f"[{self.node_id}] No hay ruta hacia {dest}")
            return
        next_hop, _ = self.routing_table[dest]
        msg["ttl"] -= 1
        if msg["ttl"] <= 0:
            return
        self.adapter.send(next_hop, msg)
        print(f"[{self.node_id}] Reenviado a {dest} via {next_hop}")

    def send_message(self, dest, payload):
        msg = {
            "proto": "lsr",
            "type": "message",
            "from": self.node_id,
            "to": dest,
            "ttl": 10,
            "headers": {},
            "payload": payload,
            "message_id": str(uuid.uuid4())[:8]
        }
        self.seen_messages.add(msg["message_id"])
        self.forward_message(msg)
