import asyncio
from config_remote import HOST, PORT, PWD, chanN
from topo_n8 import MY, NEIGHBORS_COST
from link_state_router_async import LinkStateRouterAsync

async def main():
    node = LinkStateRouterAsync(MY, NEIGHBORS_COST, HOST, PORT, PWD, print_updates=False)
    await node.start()
    print(f"[{MY}] LSR listo (LSP cada ~20s). Comandos: send N<num> <msg> | routes | lsdb | verbose on|off | quit")

    loop = asyncio.get_event_loop()
    while True:
        line = await loop.run_in_executor(None, input, "lsr> ")
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
        elif cmd == "routes":
            rt = node.routing_table
            if not rt:
                print("(vacio) aún no hay rutas calculadas")
            else:
                print("destino\t\tvia\t\tcosto")
                for d,(nh,c) in sorted(rt.items(), key=lambda x: x[0]):
                    print(f"{d}\t{nh}\t{c:.2f}")
        elif cmd == "lsdb":
            for origin, info in node.lsdb.items():
                print(f"{origin} seq={info['seq']} links={info['links']}")
        elif cmd == "verbose" and len(parts) == 2:
            node.print_updates = (parts[1].lower() == "on")
            print("print_updates:", node.print_updates)
        else:
            print("Uso: send N<numero> <mensaje> | routes | lsdb | verbose on|off | quit")

    await node.adapter.stop()

if __name__ == "__main__":
    asyncio.run(main())
