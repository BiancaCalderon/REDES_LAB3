# redis_routing/redis_adapter.py
import redis
import threading
import json

class RedisAdapter:
    def __init__(self, node_id: str, redis_host="localhost", redis_port=6379):
        self.node_id = node_id
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.running = False
        self.thread = None
        self.on_message = None  # callback

    def start(self, on_message):
       
        self.on_message = on_message
        self.running = True
        self.thread = threading.Thread(target=self._listen, daemon=True)
        self.thread.start()

    def _listen(self):
        pubsub = self.redis.pubsub()
        pubsub.subscribe(self.node_id)
        for msg in pubsub.listen():
            if not self.running:
                break
            if msg["type"] == "message":
                try:
                    data = json.loads(msg["data"])
                    if self.on_message:
                        self.on_message(data)
                except Exception as e:
                    print(f"[{self.node_id}] Error procesando mensaje: {e}")

    def send(self, dest: str, message: dict):
       
        self.redis.publish(dest, json.dumps(message))

    def stop(self):
        self.running = False
        print(f"[{self.node_id}] RedisAdapter detenido")
