import json, socket, threading, time, heapq, uuid
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set


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

    def to_json(self) -> str:
        return json.dumps({
            "proto": self.proto,
            "type": self.type,
            "from": self.from_node,
            "to": self.to_node,
            "ttl": self.ttl,
            "headers": self.headers,
            "payload": self.payload,
            "message_id": self.message_id,
        })

    @classmethod
    def from_json(cls, js: str):
        d = json.loads(js)
        return cls(
            proto=d["proto"],
            type=d["type"],
            from_node=d["from"],
            to_node=d["to"],
            ttl=d["ttl"],
            headers=d.get("headers", {}),
            payload=d.get("payload", ""),
            message_id=d.get("message_id"),
        )


class LinkStateRouter:
  
    def __init__(self, node_id: str, port: int):
        self.node_id = node_id
        self.port = port

        # Vecinos directos y sus sockets (ip, port). Solo desde topo para este nodo.
        self.neighbors: Dict[str, Tuple[str, int]] = {}

        # Costos medidos a vecinos (latencia en segundos; default=1.0 si falla ping)
        self.neighbor_costs: Dict[str, float] = {}

        # LSDB: origin -> {"seq": int, "links": {neighbor: cost}, "last_update": float}
        self.lsdb: Dict[str, Dict] = {}

        # Tabla de ruteo: destino -> (next_hop, cost_acumulado)
        self.routing_table: Dict[str, Tuple[str, float]] = {}

        # Control de ejecucion
        self.running = False
        self.socket: Optional[socket.socket] = None

        # Flood de LSP: evitar loops
        self.seen_messages: Set[str] = set()
        self.message_history_limit = 2000

        # LSP propios
        self.lsp_seq = 0
        self.lsp_refresh_sec = 20        # re-anunciar cada 20s
        self.lsp_expire_sec  = 3 * 60    
        
    # ---------- Config: cargar SOLO vecinos ----------
    def load_topology(self, topo_file: str):
        with open(topo_file, "r") as f:
            data = json.load(f)
        my_neighbors = data["config"].get(self.node_id, [])
        print(f"[{self.node_id}] Vecinos directos declarados: {my_neighbors}")

        # Mapa de puertos simple para demo local
        port_map = {"A": 8001, "B": 8002, "C": 8003, "D": 8004}
        for nb in my_neighbors:
            self.neighbors[nb] = ("localhost", port_map[nb])
            self.neighbor_costs.setdefault(nb, 1.0)

    def load_names(self, names_file: str):
       
        pass

   
    def _spf_dijkstra(self, graph: Dict[str, Dict[str, float]], source: str) -> Dict[str, Tuple[str, float]]:
        
        nodes = set(graph.keys())
        for u in list(nodes):
            nodes.update(graph[u].keys())
        nodes = list(nodes)

        dist = {n: float("inf") for n in nodes}
        prev = {n: None for n in nodes}
        dist[source] = 0.0

        pq: List[Tuple[float, str]] = [(0.0, source)]
        visited = set()

        while pq:
            d, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)

            for v, w in graph.get(u, {}).items():
                if v in visited:
                    continue
                nd = d + float(w)
                if nd < dist[v]:
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        # Construye tabla destino -> (next_hop, costo)
        table: Dict[str, Tuple[str, float]] = {}
        for dst in nodes:
            if dst == source or dist[dst] == float("inf"):
                continue
            # reconstruir path
            path = []
            cur = dst
            while cur is not None:
                path.append(cur)
                cur = prev[cur]
            path.reverse()
            if len(path) >= 2:
                table[dst] = (path[1], dist[dst])
        return table

    def _graph_from_lsdb(self) -> Dict[str, Dict[str, float]]:
     
        g: Dict[str, Dict[str, float]] = {}
        now = time.time()
       
        stale = []
        for origin, info in self.lsdb.items():
            if now - info["last_update"] > self.lsp_expire_sec:
                stale.append(origin)
        for o in stale:
            print(f"[{self.node_id}] LSDB: expira LSP de {o}")
            self.lsdb.pop(o, None)

   
        for origin, info in self.lsdb.items():
            g.setdefault(origin, {})
            for nb, cost in info["links"].items():
                g.setdefault(origin, {})
                g[origin][nb] = float(cost)
               
        return g

    def _recompute_routes(self):
        g = self._graph_from_lsdb()
        old = self.routing_table.copy()
        self.routing_table = self._spf_dijkstra(g, self.node_id)
        if old != self.routing_table:
            print(f"[{self.node_id}] Tabla de ruteo (LSR) actualizada:")
            for d, (nh, c) in sorted(self.routing_table.items()):
                print(f"  {self.node_id} -> {d} via {nh} (cost={c:.3f})")

   
    def _send_ping(self, neighbor: str):
        if neighbor not in self.neighbors:
            return
        ip, port = self.neighbors[neighbor]
        ts = time.time()
        msg = Message(
            proto="lsr", type="ping",
            from_node=self.node_id, to_node=neighbor,
            ttl=1, headers={"timestamp": ts}, payload=""
        )
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2.0)
                s.connect((ip, port))
                s.send(msg.to_json().encode())
        except:
           
            self.neighbor_costs[neighbor] = 1.0

    def _handle_ping(self, m: Message, client_socket):
        if m.type == "ping":
            resp = Message(
                proto="lsr", type="pong",
                from_node=self.node_id, to_node=m.from_node,
                ttl=1, headers=m.headers, payload=""
            )
            try:
                client_socket.send(resp.to_json().encode())
            except:
                pass
        elif m.type == "pong":
            t0 = m.headers.get("timestamp", 0.0)
            rtt = max(0.0001, time.time() - t0)
            self.neighbor_costs[m.from_node] = rtt  

    # ------------------------------ L S P --------------------------------------
    def _emit_local_lsp(self) -> Message:
      
        self.lsp_seq += 1
        links = {nb: float(self.neighbor_costs.get(nb, 1.0)) for nb in self.neighbors.keys()}
        hdr = {
            "lsp": {
                "origin": self.node_id,
                "seq": self.lsp_seq,
                "links": links,
                "age": time.time()
            }
        }
        return Message(
            proto="lsr", type="lsp",
            from_node=self.node_id, to_node="*",  # broadcast logico
            ttl=10, headers=hdr, payload=""
        )

    def _accept_and_store_lsp(self, lsp_hdr: Dict) -> bool:
       
        origin = lsp_hdr["origin"]
        seq = int(lsp_hdr["seq"])
        links = lsp_hdr.get("links", {})
        changed = False

        cur = self.lsdb.get(origin)
        if cur is None or seq > cur["seq"]:
            self.lsdb[origin] = {
                "seq": seq,
                "links": dict(links),
                "last_update": time.time()
            }
            changed = True
        else:
            
            pass
        return changed

    def _flood_to_neighbors(self, msg: Message, exclude_neighbor: Optional[str] = None):
        if msg.ttl <= 0:
            return
        msg.ttl -= 1

        sent = []
        for nb, (ip, port) in self.neighbors.items():
            if nb == exclude_neighbor:
                continue
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(3.0)
                    s.connect((ip, port))
                    s.send(msg.to_json().encode())
                    sent.append(nb)
            except Exception as e:
                
                pass
        if sent:
            print(f"[{self.node_id}] Flood LSP {msg.message_id} -> {sent}")

    
    def _forward_data(self, m: Message):
        if m.ttl <= 0:
            print(f"[{self.node_id}] TTL agotado para msg {m.message_id}")
            return
        if m.to_node == self.node_id:
            print(f"[{self.node_id}] Data de {m.from_node}: {m.payload}")
            return
        if m.to_node not in self.routing_table:
            print(f"[{self.node_id}] Sin ruta hacia {m.to_node}")
            return

        next_hop, _ = self.routing_table[m.to_node]
        if next_hop not in self.neighbors:
            print(f"[{self.node_id}] Next-hop {next_hop} no es vecino (inestable).")
            return

        ip, port = self.neighbors[next_hop]
        m.ttl -= 1
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5.0)
                s.connect((ip, port))
                s.send(m.to_json().encode())
            print(f"[{self.node_id}] a {m.to_node} via {next_hop} (msg {m.message_id})")
        except Exception as e:
            print(f"[{self.node_id}] Error enviando a {next_hop}: {e}")

   
    def _handle_client(self, client_socket, address):
        try:
            data = client_socket.recv(65536).decode()
            if not data:
                return
            m = Message.from_json(data)

            if m.message_id in self.seen_messages:
                return
            self.seen_messages.add(m.message_id)
            if len(self.seen_messages) > self.message_history_limit:
                
                self.seen_messages = set(list(self.seen_messages)[-1000:])

            if m.type in ("ping", "pong"):
                self._handle_ping(m, client_socket)

            elif m.type == "lsp":
                lsp = m.headers.get("lsp", {})
                origin = lsp.get("origin")
              
                sender_nb = None
                for nb, (ip, port) in self.neighbors.items():
                    if address[0] == ip:
                        sender_nb = nb
                        break
                updated = self._accept_and_store_lsp(lsp)
                # Flood a los demas
                self._flood_to_neighbors(m, exclude_neighbor=sender_nb)
                if updated:
                    self._recompute_routes()

            elif m.type == "message":
                self._forward_data(m)

            elif m.type in ("hello", "hello_ack"):
               
                if m.type == "hello":
                    resp = Message(proto="lsr", type="hello_ack",
                                   from_node=self.node_id, to_node=m.from_node,
                                   ttl=1, headers={}, payload="")
                    try:
                        client_socket.send(resp.to_json().encode())
                    except:
                        pass
               
            else:
                print(f"[{self.node_id}] Tipo desconocido: {m.type}")

        except Exception as e:
            print(f"[{self.node_id}] Error con cliente {address}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass

    def forwarding_process(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.socket.bind(("localhost", self.port))
            self.socket.listen(16)
            print(f"[{self.node_id}] Escuchando en {self.port}")
            while self.running:
                try:
                    c, addr = self.socket.accept()
                    t = threading.Thread(target=self._handle_client, args=(c, addr), daemon=True)
                    t.start()
                except Exception as e:
                    if self.running:
                        print(f"[{self.node_id}] Error accept(): {e}")
        finally:
            try:
                self.socket.close()
            except:
                pass

    def routing_process(self):
        time.sleep(2)  # espera a que levante el server

        # Inicial: ping a vecinos y LSP base
        for nb in list(self.neighbors.keys()):
            self._send_ping(nb)
        time.sleep(0.8)
        self._install_own_lsp_and_flood()

        last_lsp_time = 0.0
        while self.running:
            # Pingar periodicamente a vecinos para refrescar costo
            for nb in list(self.neighbors.keys()):
                self._send_ping(nb)

           
            now = time.time()
            if now - last_lsp_time > self.lsp_refresh_sec:
                self._install_own_lsp_and_flood()
                last_lsp_time = now

            time.sleep(5)

    def _install_own_lsp_and_flood(self):
        lsp_msg = self._emit_local_lsp()
       
        hdr = lsp_msg.headers["lsp"]
        self.lsdb[self.node_id] = {"seq": hdr["seq"], "links": hdr["links"], "last_update": time.time()}
        self._recompute_routes()
        
        self._flood_to_neighbors(lsp_msg)

  
    def start(self):
        self.running = True
        tf = threading.Thread(target=self.forwarding_process, daemon=True)
        tr = threading.Thread(target=self.routing_process, daemon=True)
        tf.start(); tr.start()
        print(f"[{self.node_id}] Nodo LSR iniciado.. Vecinos: {list(self.neighbors.keys())}")
        return tf, tr

    def stop(self):
        self.running = False
        if self.socket:
            try: self.socket.close()
            except: pass
        print(f"[{self.node_id}] Nodo detenido")

    def send_message(self, destination: str, content: str):
        m = Message(
            proto="lsr", type="message",
            from_node=self.node_id, to_node=destination,
            ttl=16, headers={}, payload=content
        )
        self._forward_data(m)


def main():
    import sys
    if len(sys.argv) != 2:
        print("Uso: python link_state_router.py <node_id>")
        return
    node_id = sys.argv[1]
    port_map = {"A":8001,"B":8002,"C":8003,"D":8004}
    port = port_map.get(node_id, 9000)

    r = LinkStateRouter(node_id, port)
    try:
        r.load_topology("topo.txt")  # SOLO mis vecinos
        r.load_names("names.txt")    
        r.start()

        print(f"\n[{node_id}] Comandos:\n  send <dest> <msg>\n  routes\n  quit")
        while True:
            try:
                parts = input(f"{node_id}> ").strip().split()
                if not parts: continue
                cmd = parts[0].lower()
                if cmd == "quit":
                    break
                elif cmd == "send" and len(parts) >= 3:
                    dst = parts[1]; msg = " ".join(parts[2:])
                    r.send_message(dst, msg)
                elif cmd == "routes":
                    for d,(nh,c) in sorted(r.routing_table.items()):
                        print(f"  {d} via {nh} (cost={c:.3f})")
                else:
                    print("Comando invalido revisa")
            except KeyboardInterrupt:
                break
    finally:
        r.stop()

if __name__ == "__main__":
    main()
