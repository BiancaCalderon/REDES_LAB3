# redis_adapter_async.py
import asyncio, json
import redis.asyncio as redis

class RedisAdapterAsync:
    def __init__(self, my_channel: str, host: str, port: int, password: str):
        self.my_channel = my_channel
        self.host = host
        self.port = port
        self.password = password
        self._r = None
        self._pubsub = None

    async def start(self, on_message):
        self._r = redis.Redis(host=self.host, port=self.port, password=self.password)
        self._pubsub = self._r.pubsub()
        await self._pubsub.subscribe(self.my_channel)

        async def _listen():
            async for m in self._pubsub.listen():
                if m.get("type") != "message":
                    continue
                data = m.get("data")
                try:
                    # puede venir bytes o str
                    if isinstance(data, (bytes, bytearray)):
                        data = data.decode("utf-8", errors="ignore")
                    msg = json.loads(data)
                except Exception:
                    # si alguien manda texto plano, te lo paso como string
                    msg = data
                # Pasamos "tal cual" al nodo (éste sabe desempaquetar si viene {"payload": {...}})
                await on_message(msg)

        asyncio.create_task(_listen())

    async def send(self, dst_channel: str, msg: dict):
        # publicamos JSON plano
        await self._r.publish(dst_channel, json.dumps(msg))

    async def stop(self):
        try:
            if self._pubsub:
                await self._pubsub.unsubscribe(self.my_channel)
        finally:
            if self._r:
                await self._r.aclose()
