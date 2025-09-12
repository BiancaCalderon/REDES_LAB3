# run_node_n8.py
import asyncio
import json  # Para serializar a JSON string
import redis.asyncio as redis  # Necesitas instalar redis (pip install redis) — si no, avísame
from config_remote import HOST, PORT, PWD
from node_link_state_spec import NodeLSAsync, node_to_addr

# Identidad y vecinos de N8 (según topo del profesor)
MY = "N8"
NEIGHBORS_COST = {
    "N10": 6,
    "N11": 16,
    "N3":  15,
    "N4":  8,
    "N7":  7,
}

async def main():
    print("[bootstrap] creando nodo...")
    node = NodeLSAsync(MY, NEIGHBORS_COST, HOST, PORT, PWD)
    node.verbose = False  # True para ver hellos y floods
    await node.start()

    # Cliente Redis auxiliar para envíos spoofed
    redis_client = redis.Redis(host=HOST, port=PORT, password=PWD, decode_responses=True)

    print(f"[{MY}] comandos: routes | table | lsdb | status | peek on|off | quit")
    loop = asyncio.get_event_loop()
    while True:
        line = await loop.run_in_executor(None, input, "n8> ")
        parts = line.strip().split()
        if not parts:
            continue
        cmd = parts[0].lower()
        if cmd == "quit":
            await redis_client.close()
            break
        elif cmd == "peek" and len(parts) == 2:
            node.verbose = (parts[1].lower() == "on")
            print("verbose:", node.verbose)
        elif cmd == "routes":
            node.print_routes()
        elif cmd == "table":
            node.print_table()
        elif cmd == "lsdb":
            node.print_lsdb()
        elif cmd == "status":
            me = node.node_id
            for nb in sorted(NEIGHBORS_COST.keys()):
                t = node.graph.get(me, {}).get(nb, {}).get("time")
                stat = "UP" if t and t > 0 else "DOWN"
                print(f"{nb}: {stat} ttl={int(t) if t is not None else '-'}")
        else:
            print("uso: routes | table | lsdb | status | peek on|off  | quit")

    await node.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        import traceback; traceback.print_exc()