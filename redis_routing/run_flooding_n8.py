import asyncio
from config_remote import HOST, PORT, PWD, chanN
from topo_n8 import MY, NEIGHBORS_FLOOD
from flooding_router_async import FloodingRouterAsync

async def main():
    node = FloodingRouterAsync(MY, NEIGHBORS_FLOOD, HOST, PORT, PWD)
    await node.start()
    print(f"[{MY}] listo. Comandos: send N<dest-num> <mensaje> | quit")

    loop = asyncio.get_event_loop()
    while True:
        line = await loop.run_in_executor(None, input, "flood> ")
        parts = line.strip().split()
        if not parts:
            continue
        if parts[0] == "quit":
            break
        if parts[0] == "send" and len(parts) >= 3:
            try:
                dest_n = int(parts[1].replace("N",""))
            except:
                print("Uso: send N<numero> <mensaje>")
                continue
            msg = " ".join(parts[2:])
            await node.send_message(chanN(dest_n), msg)
        else:
            print("Uso: send N<numero> <mensaje>  | quit")

    await node.adapter.stop()

if __name__ == "__main__":
    asyncio.run(main())
