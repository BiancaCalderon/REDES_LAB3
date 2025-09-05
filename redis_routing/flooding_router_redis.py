import uuid
from redis_adapter import RedisAdapter

class FloodingRouterRedis:
    def __init__(self, node_id: str, neighbors=None):
        self.node_id = node_id
        self.neighbors = neighbors or []
        self.seen_messages = set()
        self.adapter = RedisAdapter(node_id)

    def start(self):
        def on_message(msg):
            msg_id = msg.get("message_id")
            if msg_id in self.seen_messages:
                return
            self.seen_messages.add(msg_id)

            if msg["to"] == self.node_id:
                print(f"[{self.node_id}] Recibido de {msg['from']}: {msg['payload']}")
            else:
                self.flood_message(msg, exclude=msg["from"])

        self.adapter.start(on_message)
        print(f"[{self.node_id}] Nodo FloodingRedis iniciado. Vecinos: {self.neighbors}")

    def flood_message(self, msg, exclude=None):
        msg["ttl"] -= 1
        if msg["ttl"] <= 0:
            print(f"[{self.node_id}] TTL agotado")
            return

        for nb in self.neighbors:
            if nb == exclude:
                continue
            self.adapter.send(nb, msg)
        print(f"[{self.node_id}] Reenviado a {self.neighbors}")

    def send_message(self, dest, payload):
        msg = {
            "proto": "flooding",
            "type": "message",
            "from": self.node_id,
            "to": dest,
            "ttl": 10,
            "headers": {},
            "payload": payload,
            "message_id": str(uuid.uuid4())[:8]
        }
        self.seen_messages.add(msg["message_id"])
        self.flood_message(msg)

    def stop(self):
        self.adapter.stop()
