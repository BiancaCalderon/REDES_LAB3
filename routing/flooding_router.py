import json
import threading
import socket
import time
from typing import Dict, List, Set
from dataclasses import dataclass
from redis_adapter import RedisAdapter
import uuid

@dataclass
class Message:
    proto: str
    type: str
    from_node: str
    to_node: str
    ttl: int
    headers: Dict
    payload: str
    message_id: str = None

    def __post_init__(self):
        if self.message_id is None:
            self.message_id = str(uuid.uuid4())[:8]

    def to_json(self):
        return json.dumps({
            "proto": self.proto,
            "type": self.type,
            "from": self.from_node,
            "to": self.to_node,
            "ttl": self.ttl,
            "headers": self.headers,
            "payload": self.payload,
            "message_id": self.message_id
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
            payload=data["payload"],
            message_id=data.get("message_id", str(uuid.uuid4())[:8])
        )

class FloodingRouter:
    def __init__(self, node_id: str, port: int):
        self.node_id = node_id
        self.transport_type = transport
        self.running = False
        self.seen_messages = set()
        self.message_history_limit = 1000
        
        if transport == "redis":
            self.adapter = RedisAdapter(node_id)
        else:
            raise ValueError("Solo soportamos Redis en esta fase")
        
    def load_topology(self, topo_file: str):
        """Carga SOLO los vecinos directos desde archivo JSON"""
        with open(topo_file, 'r') as f:
            data = json.load(f)
            topology = data["config"]
            
            # ✅ SOLO usar vecinos directos - cumple restricción
            my_neighbors = topology.get(self.node_id, [])
            print(f"[{self.node_id}] Mis vecinos directos: {my_neighbors}")
            
            # Mapeo fijo de nodos a puertos
            port_map = {"A": 8001, "B": 8002, "C": 8003, "D": 8004}
            for neighbor in my_neighbors:
                self.neighbors[neighbor] = ("localhost", port_map[neighbor])
                
    def load_names(self, names_file: str):
        """Carga el mapeo de nombres - opcional para flooding"""
        # Flooding no necesita esto, pero lo mantenemos por compatibilidad
        pass
    
    def flood_message(self, message, exclude_node=None):
        if message["ttl"] <= 0:
            print(f"[{self.node_id}] TTL agotado")
            return

        if message["to"] == self.node_id:
            print(f"[{self.node_id}] Mensaje recibido de {message['from']}: {message['payload']}")
            return

        message["ttl"] -= 1

        for neighbor in self.neighbors:
            if neighbor == exclude_node:
                continue
            self.adapter.send(neighbor, message)
        print(f"[{self.node_id}] Flood hacia vecinos: {self.neighbors}")

    
    def handle_hello(self, message: Message, client_socket):
        """Maneja mensajes hello para descubrimiento de vecinos"""
        if message.type == "hello":
            # Responder con hello_ack
            response = Message(
                proto="flooding",
                type="hello_ack",
                from_node=self.node_id,
                to_node=message.from_node,
                ttl=1,
                headers={},
                payload=""
            )
            try:
                client_socket.send(response.to_json().encode())
                print(f"[{self.node_id}] 👋 Hello de {message.from_node} - respondido")
            except:
                pass
                
        elif message.type == "hello_ack":
            print(f"[{self.node_id}] ✅ Vecino {message.from_node} confirmado")
    
    def handle_client(self, client_socket, address):
        """Maneja conexiones entrantes"""
        try:
            data = client_socket.recv(4096).decode()
            if not data:
                return
                
            message = Message.from_json(data)
            
            # Evitar loops: verificar si ya procesamos este mensaje
            if message.message_id in self.seen_messages:
                print(f"[{self.node_id}] 🔄 Mensaje {message.message_id} ya procesado - ignorando")
                return
                
            # Recordar este mensaje
            self.seen_messages.add(message.message_id)
            # Limitar tamaño del historial
            if len(self.seen_messages) > self.message_history_limit:
                # Remover los más antiguos (simplificado)
                oldest = list(self.seen_messages)[:100]
                for old_id in oldest:
                    self.seen_messages.discard(old_id)
            
            # Procesar según tipo de mensaje
            if message.type in ["hello", "hello_ack"]:
                self.handle_hello(message, client_socket)
            elif message.type == "message":
                # Determinar de qué vecino vino el mensaje
                sender_neighbor = None
                for neighbor, (ip, port) in self.neighbors.items():
                    if address[0] == ip:  # Misma IP (simplificado)
                        sender_neighbor = neighbor
                        break
                
                # FLOODING: reenviar a todos excepto el sender
                self.flood_message(message, exclude_node=sender_neighbor)
            else:
                print(f"[{self.node_id}] ❓ Tipo de mensaje desconocido: {message.type}")
                    
        except Exception as e:
            print(f"[{self.node_id}] ❌ Error manejando cliente {address}: {e}")
        finally:
            client_socket.close()
    
    def send_hellos(self):
        """Envía hello a todos los vecinos para confirmar conectividad"""
        for neighbor_id, (ip, port) in self.neighbors.items():
            hello_msg = Message(
                proto="flooding",
                type="hello",
                from_node=self.node_id,
                to_node=neighbor_id,
                ttl=1,
                headers={},
                payload=""
            )
            
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2.0)
                    s.connect((ip, port))
                    s.send(hello_msg.to_json().encode())
            except Exception as e:
                print(f"[{self.node_id}] ❌ No pude contactar vecino {neighbor_id}: {e}")
    
    def forwarding_process(self):
        """Proceso de forwarding - escucha mensajes entrantes"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.socket.bind(("localhost", self.port))
            self.socket.listen(5)
            print(f"[{self.node_id}] 🎧 Escuchando en puerto {self.port}")
            
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
                        print(f"[{self.node_id}] ❌ Error en forwarding: {e}")
                        
        except Exception as e:
            print(f"[{self.node_id}] ❌ Error iniciando servidor: {e}")
        finally:
            if self.socket:
                self.socket.close()
    
    def routing_process(self):
        """
        Proceso de routing - para flooding es mínimo
        Solo verifica conectividad con vecinos periódicamente
        """
        # Esperar a que el servidor esté listo
        time.sleep(2)
        
        # Enviar hellos iniciales
        print(f"[{self.node_id}] 🔍 Verificando conectividad con vecinos...")
        self.send_hellos()
        
        while self.running:
            # Verificar vecinos cada 2 minutos (menos spam)
            time.sleep(120)
            self.send_hellos()
    
    def start(self):
        self.running = True

        def on_message(msg):
            msg_id = msg.get("message_id")
            if msg_id in self.seen_messages:
                return
            self.seen_messages.add(msg_id)

            if msg["type"] == "message":
                self.flood_message(msg, exclude_node=msg["from"])
            elif msg["type"] in ["hello", "hello_ack"]:
                print(f"[{self.node_id}] Mensaje hello: {msg}")

        self.adapter.start(on_message)
        print(f"[{self.node_id}] Nodo Flooding iniciado con Redis")


    
    def stop(self):
        """Detiene el nodo"""
        self.running = False
        if self.socket:
            self.socket.close()
        print(f"[{self.node_id}] 🛑 Nodo detenido")
    
    def send_message(self, destination: str, content: str):
        """
        Envía un mensaje a un destino usando flooding
        No necesita tabla de enrutamiento - flooding se encarga
        """
        message = Message(
            proto="flooding",
            type="message",
            from_node=self.node_id,
            to_node=destination,
            ttl=10,  # TTL generoso para flooding
            headers={},
            payload=content
        )
        
        # Marcar como visto ANTES de enviar para evitar que nos llegue de vuelta
        self.seen_messages.add(message.message_id)
        
        print(f"[{self.node_id}] 📤 Iniciando flooding de mensaje a {destination}")
        self.flood_message(message)

# Ejemplo de uso
def main():
    import sys
    
    if len(sys.argv) != 2:
        print("Uso: python flooding_router.py <node_id>")
        return
    
    node_id = sys.argv[1]
    router = FloodingRouter(node_id, transport="redis")
    
    try:
        router.start()
        print(f"\n[{node_id}] Comandos:\n  send <dest> <msg>\n  quit")

        while True:
            cmd = input(f"{node_id}> ").strip().split()
            if not cmd:
                continue
            if cmd[0] == "quit":
                break
            elif cmd[0] == "send" and len(cmd) >= 3:
                dest = cmd[1]
                msg = " ".join(cmd[2:])
                router.adapter.send(dest, {
                    "proto": "flooding",
                    "type": "message",
                    "from": node_id,
                    "to": dest,
                    "ttl": 10,
                    "headers": {},
                    "payload": msg
                })
            else:
                print("Comando inválido")

    except KeyboardInterrupt:
        pass
    finally:
        router.adapter.stop()

if __name__ == "__main__":
    main()