import asyncio
from config_remote import HOST, PORT, PWD, chanN
from topo_n8 import MY, NEIGHBORS_COST
from link_state_router_async import LinkStateRouterAsync

async def main():
    node = LinkStateRouterAsync(MY, NEIGHBORS_COST, HOST, PORT, PWD)
    await node.start()
    print(f"[{MY}] LSR listo (LSP cada ~20s). Comandos: send N<num> <msg> | quit")

    loop = asyncio.get_event_loop()
    while True:
        line = await loop.run_in_executor(None, input, "lsr> ")
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
