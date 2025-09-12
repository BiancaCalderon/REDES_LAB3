import asyncio, time
from config_remote import HOST, PORT, PWD, chanN
from topo_n8 import MY, NEIGHBORS_FLOOD
from flooding_router_async import FloodingRouterAsync

async def main():
    node = FloodingRouterAsync(MY, NEIGHBORS_FLOOD, HOST, PORT, PWD)
    await node.start()
    print(f"[{MY}] listo. Comandos: send N<dest> <mensaje> | neighbors | quit")

    loop = asyncio.get_event_loop()
    while True:
        line = await loop.run_in_executor(None, input, "flood> ")
        parts = line.strip().split()
        if not parts:
            continue
        cmd = parts[0].lower()
        if cmd == "quit":
            break
        elif cmd == "send" and len(parts) >= 3:
            try:
                dest_n = int(parts[1].replace("n","").replace("N",""))
            except:
                print("Uso: send N<numero> <mensaje>")
                continue
            msg = " ".join(parts[2:])
            await node.send_message(chanN(dest_n), msg)
        elif cmd == "neighbors":
            for nb, st in node.neighbors_status().items():
                if st["last_seen"]:
                    age = time.time() - st["last_seen"]
                    print(f"{nb}: {'UP' if st['alive'] else 'DOWN'} (last_seen={age:.1f}s)")
                else:
                    print(f"{nb}: DOWN (never)")
        else:
            print("Uso: send N<numero> <mensaje> | neighbors | quit")

    await node.adapter.stop()

if __name__ == "__main__":
    asyncio.run(main())
