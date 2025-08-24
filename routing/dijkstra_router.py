import json
import heapq
import threading
import socket
import time
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict

@dataclass
class Message:
    proto: str
    type: str
    from_node: str
    to_node: str
    ttl: int
    headers: Dict
    payload: str

    def to_json(self):
        return json.dumps({
            "proto": self.proto,
            "type": self.type,
            "from": self.from_node,
            "to": self.to_node,
            "ttl": self.ttl,
            "headers": self.headers,
            "payload": self.payload
        })

    @classmethod
    def from_json(cls, json_str: str):
        data = json.loads(json_str)
        return cls(
            proto=data["proto"],
            type=data["type"],
            from_node=data["from"],
            to_node=data["to"],
            ttl=data["ttl"],
            headers=data.get("headers", {}),
            payload=data["payload"]
        )

class DijkstraRouter:
    def __init__(self, node_id: str, port: int):
        self.node_id = node_id
        self.port = port
        self.topology = {}  # {node: {neighbor: weight}}
        self.routing_table = {}  # {destination: (next_hop, cost)}
        self.neighbors = {}  # {neighbor_id: (ip, port)}
        self.running = False
        self.socket = None
        
        # Para medir latencias
        self.ping_times = {}  # {neighbor: latency}
        
    def load_topology(self, topo_file: str):
        """Carga la topología desde archivo JSON"""
        with open(topo_file, 'r') as f:
            data = json.load(f)
            self.topology = data["config"]
            
    def load_names(self, names_file: str):
        """Carga el mapeo de nombres a direcciones"""
        with open(names_file, 'r') as f:
            data = json.load(f)
            # Mapeo fijo de nodos a puertos
            port_map = {"A": 8001, "B": 8002, "C": 8003, "D": 8004}
            for node in data["config"].keys():
                if node != self.node_id:
                    self.neighbors[node] = ("localhost", port_map[node])
    
    def dijkstra_algorithm(self, source: str) -> Dict[str, Tuple[str, float]]:
        """
        Implementa el algoritmo de Dijkstra
        Retorna: {destination: (next_hop, distance)}
        """
        # Inicializar distancias
        distances = {node: float('inf') for node in self.topology.keys()}
        distances[source] = 0
        
        # Registro del camino más corto
        previous = {node: None for node in self.topology.keys()}
        
        # Cola de prioridad: (distancia, nodo)
        pq = [(0, source)]
        visited = set()
        
        while pq:
            current_dist, current = heapq.heappop(pq)
            
            if current in visited:
                continue
                
            visited.add(current)
            
            # Revisar vecinos
            for neighbor in self.topology.get(current, []):
                if neighbor in visited:
                    continue
                    
                # Obtener peso del enlace (usar ping o peso por defecto)
                weight = self.ping_times.get(neighbor, 1.0)
                new_dist = current_dist + weight
                
                if new_dist < distances[neighbor]:
                    distances[neighbor] = new_dist
                    previous[neighbor] = current
                    heapq.heappush(pq, (new_dist, neighbor))
        
        # Construir tabla de enrutamiento
        routing_table = {}
        for dest in self.topology.keys():
            if dest != source and distances[dest] != float('inf'):
                # Encontrar el siguiente salto
                path = []
                current = dest
                while current is not None:
                    path.append(current)
                    current = previous[current]
                path.reverse()
                
                if len(path) > 1:
                    next_hop = path[1]  # El siguiente después del source
                    routing_table[dest] = (next_hop, distances[dest])
        
        return routing_table
    
    def update_routing_table(self):
        """Actualiza la tabla de enrutamiento usando Dijkstra"""
        old_table = self.routing_table.copy()
        self.routing_table = self.dijkstra_algorithm(self.node_id)
        
        # Solo mostrar si hay cambios
        if old_table != self.routing_table:
            print(f"[{self.node_id}] Tabla de enrutamiento actualizada:")
            for dest, (next_hop, cost) in self.routing_table.items():
                print(f"  {dest} -> via {next_hop} (cost: {cost:.2f})")
    
    def send_ping(self, neighbor: str):
        """Envía ping a un vecino para medir latencia"""
        if neighbor not in self.neighbors:
            return
            
        start_time = time.time()
        message = Message(
            proto="dijkstra",
            type="ping",
            from_node=self.node_id,
            to_node=neighbor,
            ttl=1,
            headers={"timestamp": start_time},
            payload=""
        )
        
        try:
            ip, port = self.neighbors[neighbor]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2.0)  # Timeout de 2 segundos
                s.connect((ip, port))
                s.send(message.to_json().encode())
        except ConnectionRefusedError:
            # Nodo no disponible, usar peso por defecto
            self.ping_times[neighbor] = 1.0
        except Exception as e:
            print(f"[{self.node_id}] Error enviando ping a {neighbor}: {e}")
            self.ping_times[neighbor] = 1.0
    
    def handle_ping(self, message: Message, client_socket):
        """Maneja mensajes ping"""
        if message.type == "ping":
            # Responder con pong
            response = Message(
                proto="dijkstra",
                type="pong",
                from_node=self.node_id,
                to_node=message.from_node,
                ttl=1,
                headers=message.headers,
                payload=""
            )
            client_socket.send(response.to_json().encode())
            
        elif message.type == "pong":
            # Calcular latencia
            start_time = message.headers.get("timestamp", 0)
            latency = time.time() - start_time
            self.ping_times[message.from_node] = latency
            # Solo mostrar pings cuando hay cambios significativos
            # print(f"[{self.node_id}] Ping a {message.from_node}: {latency*1000:.2f}ms")
    
    def forward_message(self, message: Message):
        """Reenvía un mensaje usando la tabla de enrutamiento"""
        destination = message.to_node
        
        # Verificar TTL
        if message.ttl <= 0:
            print(f"[{self.node_id}] TTL agotado para mensaje a {destination}")
            return
            
        # Si el mensaje es para este nodo
        if destination == self.node_id:
            print(f"[{self.node_id}] Mensaje recibido de {message.from_node}: {message.payload}")
            return
            
        # Buscar ruta en tabla de enrutamiento
        if destination not in self.routing_table:
            print(f"[{self.node_id}] No hay ruta a {destination}")
            return
            
        next_hop, _ = self.routing_table[destination]
        
        # Decrementar TTL
        message.ttl -= 1
        
        # Reenviar al siguiente salto
        if next_hop in self.neighbors:
            try:
                ip, port = self.neighbors[next_hop]
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5.0)
                    s.connect((ip, port))
                    s.send(message.to_json().encode())
                    print(f"[{self.node_id}] Mensaje para {destination} reenviado via {next_hop}")
            except Exception as e:
                print(f"[{self.node_id}] Error reenviando a {next_hop}: {e}")
        else:
            print(f"[{self.node_id}] Vecino {next_hop} no encontrado")
    
    def handle_client(self, client_socket, address):
        """Maneja conexiones entrantes"""
        try:
            data = client_socket.recv(4096).decode()
            if data:
                message = Message.from_json(data)
                
                if message.type in ["ping", "pong"]:
                    self.handle_ping(message, client_socket)
                elif message.type == "message":
                    self.forward_message(message)
                else:
                    print(f"[{self.node_id}] Tipo de mensaje desconocido: {message.type}")
                    
        except Exception as e:
            print(f"[{self.node_id}] Error manejando cliente {address}: {e}")
        finally:
            client_socket.close()
    
    def forwarding_process(self):
        """Proceso de forwarding - escucha mensajes entrantes"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.socket.bind(("localhost", self.port))
            self.socket.listen(5)
            print(f"[{self.node_id}] Escuchando en puerto {self.port}")
            
            while self.running:
                try:
                    client_socket, address = self.socket.accept()
                    # Manejar cada conexión en un hilo separado
                    thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address)
                    )
                    thread.daemon = True
                    thread.start()
                except Exception as e:
                    if self.running:
                        print(f"[{self.node_id}] Error en forwarding: {e}")
                        
        except Exception as e:
            print(f"[{self.node_id}] Error iniciando servidor: {e}")
        finally:
            if self.socket:
                self.socket.close()
    
    def routing_process(self):
        """Proceso de routing - actualiza tablas periódicamente"""
        # Inicialización inicial con más tiempo
        time.sleep(3)
        
        while self.running:
            # Enviar pings solo a vecinos directos
            direct_neighbors = self.topology.get(self.node_id, [])
            for neighbor in direct_neighbors:
                self.send_ping(neighbor)
            
            # Esperar respuestas de ping
            time.sleep(2)
            
            # Actualizar tabla de enrutamiento
            self.update_routing_table()
            
            # Esperar antes del próximo ciclo (más tiempo para reducir spam)
            time.sleep(30)
    
    def start(self):
        """Inicia el nodo"""
        self.running = True
        
        # Inicializar tabla de enrutamiento
        self.update_routing_table()
        
        # Iniciar procesos en hilos separados
        forwarding_thread = threading.Thread(target=self.forwarding_process)
        routing_thread = threading.Thread(target=self.routing_process)
        
        forwarding_thread.daemon = True
        routing_thread.daemon = True
        
        forwarding_thread.start()
        routing_thread.start()
        
        print(f"[{self.node_id}] Nodo iniciado")
        
        return forwarding_thread, routing_thread
    
    def stop(self):
        """Detiene el nodo"""
        self.running = False
        if self.socket:
            self.socket.close()
        print(f"[{self.node_id}] Nodo detenido")
    
    def send_message(self, destination: str, content: str):
        """Envía un mensaje a un destino"""
        message = Message(
            proto="dijkstra",
            type="message",
            from_node=self.node_id,
            to_node=destination,
            ttl=10,
            headers={},
            payload=content
        )
        
        self.forward_message(message)

# Ejemplo de uso
def main():
    import sys
    
    if len(sys.argv) != 2:
        print("Uso: python dijkstra_router.py <node_id>")
        return
    
    node_id = sys.argv[1]
    
    # Puerto base + offset según el nodo
    port_map = {"A": 8001, "B": 8002, "C": 8003, "D": 8004}
    port = port_map.get(node_id, 8000)
    
    router = DijkstraRouter(node_id, port)
    
    try:
        # Cargar configuración
        router.load_topology("topo.txt")
        router.load_names("names.txt")
        
        # Iniciar nodo
        router.start()
        
        # Interfaz simple para enviar mensajes
        print(f"\n[{node_id}] Comandos:")
        print("  send <destino> <mensaje>")
        print("  quit")
        
        while True:
            try:
                cmd = input(f"{node_id}> ").strip().split()
                
                if not cmd:
                    continue
                    
                if cmd[0] == "quit":
                    break
                elif cmd[0] == "send" and len(cmd) >= 3:
                    dest = cmd[1]
                    msg = " ".join(cmd[2:])
                    router.send_message(dest, msg)
                else:
                    print("Comando inválido")
                    
            except KeyboardInterrupt:
                break
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        router.stop()

if __name__ == "__main__":
    main()