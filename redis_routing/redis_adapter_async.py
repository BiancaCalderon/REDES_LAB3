import asyncio, json, redis.asyncio as redis

class RedisAdapterAsync:
    def __init__(self, channel, host, port, password):
        self.channel = channel
        self.r = redis.Redis(host=host, port=port, password=password, decode_responses=False)
        self._pubsub = None
        self._task = None
        self._on_message = None

    async def start(self, on_message):
        self._on_message = on_message
        self._pubsub = self.r.pubsub()
        await self._pubsub.subscribe(self.channel)
        self._task = asyncio.create_task(self._listen())
        print(f"[{self.channel}] Subscrito en canal remoto")

    async def stop(self):
        try:
            if self._pubsub:
                await self._pubsub.unsubscribe(self.channel)
                await self._pubsub.close()
        finally:
            self._pubsub = None
        if self._task:
            self._task.cancel()
            self._task = None
        await self.r.close()
        print(f"[{self.channel}] Desconectado")

    async def send(self, dest_channel: str, message):
        # se publica en el canal destino
        if isinstance(message, (dict, list)):
            payload = json.dumps(message).encode()
        elif isinstance(message, str):
            payload = message.encode()
        else:
            payload = json.dumps(message).encode()
        await self.r.publish(dest_channel, payload)

    async def _listen(self):
        while True:
            msg = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not msg:
                await asyncio.sleep(0.01)
                continue
            data = msg.get("data")
            if isinstance(data, (bytes, bytearray)):
                try:
                    obj = json.loads(data.decode())
                except Exception:
                    obj = data.decode(errors="ignore")
            else:
                obj = data
            if self._on_message:
                await self._on_message(obj)
