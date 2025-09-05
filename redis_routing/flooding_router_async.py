import uuid, asyncio
from redis_adapter_async import RedisAdapterAsync

class FloodingRouterAsync:
    def __init__(self, node_id, neighbors, host, port, password):
        self.node_id = node_id
        self.neighbors = neighbors
        self.adapter = RedisAdapterAsync(node_id, host=host, port=port, password=password)
        self.seen = set()

    async def start(self):
        async def on_message(msg):
            mid = msg.get("message_id")
            if mid in self.seen:
                return
            self.seen.add(mid)

            if msg["to"] == self.node_id:
                print(f"[{self.node_id}] Recibido de {msg['from']}: {msg['payload']}")
            else:
                await self.flood(msg, exclude=msg["from"])

        await self.adapter.start(on_message)

    async def flood(self, msg, exclude=None):
        msg["ttl"] -= 1
        if msg["ttl"] <= 0:
            return
        for nb in self.neighbors:
            if nb == exclude:
                continue
            await self.adapter.send(nb, msg)
            print(f"[{self.node_id}] Reenviado a {nb}")

    async def send_message(self, dest, payload):
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
        self.seen.add(msg["message_id"])
        await self.flood(msg)
