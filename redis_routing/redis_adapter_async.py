
import asyncio
import json
import redis.asyncio as redis

class RedisAdapterAsync:
    def __init__(self, node_id: str, host="localhost", port=6379, password=None):
        self.node_id = node_id
        self.redis = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        self.running = False

    async def start(self, on_message):
        # empieza a escuchar mensajes en el canal del nodo
        self.running = True
        await self.pubsub.subscribe(self.node_id)
        asyncio.create_task(self._listen(on_message))
        print(f"[{self.node_id}] Subscrito en canal remoto")

    async def _listen(self, on_message):
        while self.running:
            msg = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg:
                try:
                    data = json.loads(msg["data"])
                except:
                    data = msg["data"]
                await on_message(data)

    async def send(self, dest: str, message: dict):
        await self.redis.publish(dest, json.dumps(message))

    async def stop(self):
        self.running = False
        await self.pubsub.unsubscribe(self.node_id)
        await self.pubsub.close()
        await self.redis.close()
        print(f"[{self.node_id}] Desconectado")
