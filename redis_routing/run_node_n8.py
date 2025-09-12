import asyncio
from config_remote import HOST, PORT, PWD
from topo_n8 import MY, NEIGHBORS
from node_link_state_async import NodeLSAsync

async def main():
    node = NodeLSAsync(MY, NEIGHBORS, HOST, PORT, PWD)
    await node.start()
    print(f"[{MY}] comandos: routes | table | quit")

    loop = asyncio.get_event_loop()
    while True:
        line = await loop.run_in_executor(None, input, "n8> ")
        cmd = line.strip().lower()
        if cmd == "quit":
            break
        elif cmd == "routes":
            rt = node.routing_table
            if not rt:
                print("(vacío) aún no hay rutas")
            else:
                print("destino\t\tvia\t\tcosto")
                for d,(nh,c) in sorted(rt.items(), key=lambda x: x[0]):
                    print(f"{d}\t{nh}\t{c:.2f}")
        elif cmd == "table":
            
            for u, adj in node.topology.items():
                for v, meta in adj.items():
                    print(f"{u} -> {v}  w={meta['weight']}  t={meta['time']}")
        else:
            print("uso: routes | table | quit")

    await node.adapter.stop()

if __name__ == "__main__":
    asyncio.run(main())
