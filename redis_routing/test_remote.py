import asyncio
from flooding_router_async import FloodingRouterAsync
from dijkstra_router_async import DijkstraRouterAsync
from link_state_router_async import LinkStateRouterAsync

HOST = "lab3.redesuvg.cloud"
PORT = 6379
PWD  = "UVGRedis2025"

async def main():
    
    A = FloodingRouterAsync("sec30.grupo5.nodo1", ["sec30.grupo5.nodo2"], HOST, PORT, PWD)
    B = FloodingRouterAsync("sec30.grupo5.nodo2", ["sec30.grupo5.nodo1","sec30.grupo5.nodo3"], HOST, PORT, PWD)

    await A.start()
    await B.start()
    await asyncio.sleep(2)
    await A.send_message("sec30.grupo5.nodo2", "Hola desde nodo1 remoto!")
    await asyncio.sleep(5)
    await A.adapter.stop(); await B.adapter.stop()

if __name__ == "__main__":
    asyncio.run(main())
