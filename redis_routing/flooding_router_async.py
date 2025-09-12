import uuid, asyncio, time
from redis_adapter_async import RedisAdapterAsync

class FloodingRouterAsync:
    def __init__(self, node_id: str, neighbors, host, port, password):
        """
        node_id: canal propio (ej: sec30.grupo8.nodo8)
        neighbors: lista de canales vecinos
        """
        self.node_id = node_id
        self.neighbors = list(neighbors)
        self.adapter = RedisAdapterAsync(node_id, host=host, port=port, password=password)
        self.seen = set()               # msg ids vistos
        self.last_seen = {}             # vecino -> timestamp de último hello/ack
        self.hello_interval = 15        # segundos

    async def start(self):
        async def on_message(msg):
            # Ignorar no-JSON (texto plano), sin romper
            if not isinstance(msg, dict):
                return

            mid = msg.get("message_id")
            if mid in self.seen:
                return
            self.seen.add(mid)

            t = msg.get("type")

            if t == "message":
                if msg.get("to") == self.node_id:
                    print(f"[{self.node_id}] 🎯 Recibido de {msg.get('from')}: {msg.get('payload')}")
                else:
                    # Reenviar a TODOS los vecinos excepto el emisor
                    await self.flood(msg, exclude=msg.get("from"))

            elif t == "hello":
                # keepalive
                self.last_seen[msg.get("from")] = time.time()
                ack = {
                    "proto":"flooding","type":"hello_ack",
                    "from": self.node_id, "to": msg.get("from"),
                    "ttl": 1, "headers":{}, "payload":"",
                    "message_id": str(uuid.uuid4())[:8]
                }
                await self.adapter.send(msg.get("from"), ack)

            elif t == "hello_ack":
                self.last_seen[msg.get("from")] = time.time()

            else:
                # otros tipos: ignorar
                return

        await self.adapter.start(on_message)
        asyncio.create_task(self._hello_loop())
        print(f"[{self.node_id}] 🚀 Nodo FloodingRedis iniciado. Vecinos: {self.neighbors}")

    async def flood(self, msg: dict, exclude=None):
        # TTL
        msg["ttl"] = msg.get("ttl", 10) - 1
        if msg["ttl"] <= 0:
            return
        sent = []
        for nb in self.neighbors:
            if nb == exclude:
                continue
            await self.adapter.send(nb, msg)
            sent.append(nb)
        if sent:
            print(f"[{self.node_id}] 📡 Reenviado a {sent}")

    async def send_message(self, dest: str, payload: str):
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
        # Marcar visto para evitar rebote hacia mí
        self.seen.add(msg["message_id"])
        await self.flood(msg)

    async def _hello_loop(self):
        while True:
            await asyncio.sleep(self.hello_interval)
            for nb in self.neighbors:
                hello = {
                    "proto":"flooding","type":"hello",
                    "from": self.node_id, "to": nb,
                    "ttl": 1, "headers":{}, "payload":"",
                    "message_id": str(uuid.uuid4())[:8]
                }
                await self.adapter.send(nb, hello)

    def neighbors_status(self):
        now = time.time()
        status = {}
        for nb in self.neighbors:
            last = self.last_seen.get(nb, 0)
            alive = (now - last) < (self.hello_interval * 2.5)
            status[nb] = {"alive": alive, "last_seen": last}
        return status
