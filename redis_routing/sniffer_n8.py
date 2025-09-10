import asyncio, redis.asyncio as redis
from config_remote import HOST, PORT, PWD, chanN

MY = chanN(8)  # sec30.grupo8.nodo8

async def main():
    r = redis.Redis(host=HOST, port=PORT, password=PWD, decode_responses=True)
    async with r.pubsub() as ps:
        await ps.subscribe(MY)
        print(f"[{MY}] 👂 escuchando...")
        while True:
            m = await ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if m:
                print(f"[{MY}] RX bruto: {m}")

if __name__ == "__main__":
    asyncio.run(main())
