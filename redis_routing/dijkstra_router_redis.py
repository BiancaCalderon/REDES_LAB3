import heapq, uuid
from redis_adapter import RedisAdapter
import time, threading, uuid

class DijkstraRouterRedis:
    def __init__(self, node_id: str, neighbors: dict):
     
        self.node_id = node_id
        self.neighbors = neighbors
        self.topology = {node_id: neighbors.copy()} 
        self.routing_table = {}
        self.seen_messages = set()
        self.adapter = RedisAdapter(node_id)
        self.update_interval = 15

    def start(self):
        def on_message(msg):
            msg_id = msg.get("message_id")
            if msg_id in self.seen_messages:
                return
            self.seen_messages.add(msg_id)

            t = msg["type"]
            if t == "message":
                if msg["to"] == self.node_id:
                    print(f"[{self.node_id}] Recibido de {msg['from']}: {msg['payload']}")
                else:
                    self.forward_message(msg)
            elif t == "topo_update":
                self.handle_topology_update(msg)
             
                origin = msg["from"]
                for nb in self.neighbors:
                    if nb != origin:
                        self.adapter.send(nb, msg)

        self.adapter.start(on_message)
        # Emite tu topo local a vecinos
        self._emit_topology_update()
        
        threading.Thread(target=self._periodic_updates, daemon=True).start()
        self.update_routing_table()
        print(f"[{self.node_id}] Nodo DijkstraRedis iniciado. Vecinos: {list(self.neighbors.keys())}")

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

    def forward_message(self, msg):
        dest = msg["to"]
        if dest not in self.routing_table:
            print(f"[{self.node_id}] No hay ruta a {dest}")
            return
        next_hop, _ = self.routing_table[dest]
        msg["ttl"] -= 1
        if msg["ttl"] <= 0:
            print(f"[{self.node_id}] TTL agotado para {dest}")
            return
        self.adapter.send(next_hop, msg)
        print(f"[{self.node_id}] Reenviado hacia {dest} via {next_hop}")

    def handle_topology_update(self, msg):
        topo_info = msg.get("headers", {}).get("topo", {})
        origin = msg["from"]
        self.topology.setdefault(origin, {})
        self.topology[origin] = topo_info
        self.update_routing_table()

    def send_message(self, dest, payload):
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
        self.seen_messages.add(msg["message_id"])
        self.forward_message(msg)
        
    
    def _emit_topology_update(self):
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
        self.seen_messages.add(msg["message_id"])
        for nb in self.neighbors:
            self.adapter.send(nb, msg)
            
    def _periodic_updates(self):
        while True:
            time.sleep(self.update_interval)
            self._emit_topology_update()
