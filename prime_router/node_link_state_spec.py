# node_link_state_spec.py
import asyncio, json, time, math, heapq
from typing import Dict, Any, Optional
from redis_adapter_async import RedisAdapterAsync

# ------------------ helpers de direcciones ------------------

def _safe_weight(x: object, default: Optional[float] = None) -> Optional[float]:
    """
    Intenta convertir x a float. Devuelve None si es inválido
    (None, NaN, +/-inf, string rara, negativo, etc.).
    Si quieres un valor por defecto, pásalo en 'default'.
    """
    try:
        if x is None:
            return default
        w = float(x)
        if not math.isfinite(w) or w < 0:
            return default
        return w
    except Exception:
        return default

def node_to_addr(n: str) -> str:
    """'N8' -> 'sec30.grupo8.nodo8'."""
    try:
        i = int(n.replace("N", ""))
        return f"sec30.grupo{i}.nodo{i}"
    except Exception:
        return n

def addr_to_node(addr: str) -> str:
    """'sec30.grupo8.nodo8' -> 'N8'."""
    try:
        last = addr.split(".")[-1]   # "nodo8"
        i = int(last.replace("nodo", ""))
        return f"N{i}"
    except Exception:
        return addr


class NodeLSAsync:
    """
    Link-State (HELLO + flooding 'message' + Dijkstra) siguiendo la spec.
    - Solo enlaces locales con TTL (HELLO) se consideran vivos para re-anunciar.
    - Los 'message' de flooding actualizan la LSDB, pero NO pisan el peso local.
    - Dijkstra sobre el grafo consolidado.
    """
    def __init__(self, node_id: str, neighbors_cost: Dict[str, float], host: str, port: int, password: str):
        # Identidad
        self.node_id: str = node_id                     # "N8"
        self.me_addr: str = node_to_addr(node_id)       # "sec30.grupo8.nodo8"

        # Vecinos directos (en N# y en addr)
        self.neighbors_cost_n: Dict[str, float] = {n: float(w) for n, w in neighbors_cost.items()}  # "N7":7, ...
        self.neighbors_cost_addr: Dict[str, float] = {node_to_addr(n): float(w) for n, w in neighbors_cost.items()}

        # Transporte Redis
        self.adapter = RedisAdapterAsync(self.me_addr, host=host, port=port, password=password)

        # Timers / intervals
        self.hello_interval: float = 3.0
        self.announce_interval: float = 6.0
        self.neighbor_ttl_default: int = 30
        self.loop_sleep: float = 1.0
        self.node_dead: float = 15.0
        self.last_seen: Dict[str, float] = {}

        # Estado topo + rutas
        # grafo: "N8" -> {"N7": {"weight": 7, "time": 30}, ...}
        # 'time' solo en enlaces que salen de self.node_id (enlaces directos)
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = {}
        self.routes: Dict[str, tuple[str, float]] = {}

        # Varios
        self.verbose: bool = False
        self._tasks: list[asyncio.Task] = []

        # Inicializa tus enlaces locales con TTL
        self._install_local_neighbors()

    # --------------------------- ciclo de vida ----------------------------

    async def start(self):
        async def on_raw(raw):
            msg = self._normalize_incoming(raw)
            if not msg:
                return
            await self._handle_msg(msg, raw)

        await self.adapter.start(on_raw)
        print(f"[{self.me_addr}] Subscrito en canal remoto")

        # Primer HELLO + anuncio inicial de enlaces vivos
        await self._send_hello_all()
        await self._announce_local_links()

        # Loops
        self._tasks = [
            asyncio.create_task(self._hello_loop()),
            asyncio.create_task(self._aging_loop()),
        ]

        print(f"[{self.me_addr}] Nodo LSR (spec) iniciado. Vecinos: {list(self.neighbors_cost_addr.keys())}")

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        await self.adapter.stop()

    # ---------------------------- IO / Redis -----------------------------

    def _normalize_incoming(self, raw: Any) -> Optional[Dict[str, Any]]:
        """
        Soporta:
        - {"type","from","to","hops"}
        - {"sender_id": "...", "payload": { ... }}
        - json string con lo anterior
        """
        if isinstance(raw, dict) and "payload" in raw and isinstance(raw["payload"], dict):
            msg = raw["payload"]
            msg.setdefault("from", raw.get("sender_id", self.me_addr))
            return msg
        if isinstance(raw, dict) and "type" in raw:
            return raw
        if isinstance(raw, str):
            try:
                d = json.loads(raw)
                if isinstance(d, dict) and "type" in d:
                    return d
            except Exception:
                return None
        return None

    # ------------------------- instalación local -------------------------

    def _install_local_neighbors(self):
        """Crea/actualiza N8->vecino (con TTL) y vecino->N8 (sin TTL) en el grafo."""
        me = self.node_id
        self.graph.setdefault(me, {})
        for nb_n, w in self.neighbors_cost_n.items():
            self.graph[me][nb_n] = {"weight": float(w), "time": self.neighbor_ttl_default}
            self.graph.setdefault(nb_n, {})
            self.graph[nb_n][me] = {"weight": float(w)}  # sin 'time'
        self._recompute_spf()

    # ------------------------- envío periódico ---------------------------

    async def _hello_loop(self):
        while True:
            await self._send_hello_all()        # HELLO cada 3s
            await self._announce_local_links()  # MESSAGE cada 3s (pegado al hello)
            await asyncio.sleep(self.hello_interval)

    async def _announce_loop(self):
        while True:
            await self._announce_local_links()
            await asyncio.sleep(self.announce_interval)

    async def _aging_loop(self):
        loop = asyncio.get_event_loop()
        while True:
            me = self.node_id
            changed = False

            # 1) Enlaces directos: decremento TTL (como ya lo tienes)
            row = self.graph.get(me, {})
            for nb in list(row.keys()):
                if nb == me:
                    continue
                if "time" in row[nb]:
                    row[nb]["time"] = max(0, row[nb]["time"] - self.loop_sleep)
                    if row[nb]["time"] <= 0:
                        if self.verbose:
                            print(f"[{self.me_addr}] 🔻 expira {me}->{nb}")
                        row.pop(nb, None)
                        self.graph.get(nb, {}).pop(me, None)
                        changed = True

            # 2) Nodos **no vecinos**: expiran si no se “ven” en node_dead
            now = loop.time()
            direct_neighbors = set(self.neighbors_cost_n.keys())
            # candidatos = nodos presentes en el grafo, excepto yo y mis vecinos directos
            candidates = set(self.graph.keys())
            for u in list(self.graph.keys()):
                candidates |= set(self.graph[u].keys())
            candidates.discard(me)
            candidates -= direct_neighbors

            for n in list(candidates):
                last = self.last_seen.get(n, 0.0)
                if (now - last) > self.node_dead:
                    if self._purge_node_everywhere(n):
                        changed = True
                        if self.verbose:
                            print(f"[{self.me_addr}] 🔻 expira remoto {n} (> {self.node_dead}s)")

            if changed:
                self._recompute_spf()
                await self._announce_local_links()

            await asyncio.sleep(self.loop_sleep)


    async def _send_hello_all(self):
        for nb_n, w in self.neighbors_cost_n.items():
            w_s = _safe_weight(w, default=1.0)
            if w_s is None:
                # Si por alguna razón el costo local es inválido, no envíes
                if self.verbose:
                    print(f"[{self.me_addr}] (skip HELLO) peso inválido hacia {nb_n}: {w}")
                continue
            wire = {"type": "hello", "from": self.me_addr, "to": node_to_addr(nb_n), "hops": w_s}
            await self.adapter.send(node_to_addr(nb_n), wire)
            if self.verbose:
                print(f"[{self.me_addr}] → HELLO {node_to_addr(nb_n)} (hops={w_s})")


    async def _announce_local_links(self):
        """
        Propaga (type='message') solo enlaces LOCALES vivos (TTL>0):
        N8 <-> Nb con su 'weight' local.
        """
        me = self.node_id
        row = self.graph.get(me, {})
        alive = [(nb, d["weight"]) for nb, d in row.items() if nb != me and d.get("time", 0) > 0]
        if not alive:
            return
        for nb, w in alive:
            w_s = _safe_weight(w, default=None)
            if w_s is None:
                if self.verbose:
                    print(f"[{self.me_addr}] ↪ ANNOUNCE: {self.node_id}<->{nb} w={w}")
                    print(f"[{self.me_addr}] (skip announce) peso inválido para {self.node_id}<->{nb}: {w}")
                continue
            msg = {"type": "message", "from": self.me_addr, "to": node_to_addr(nb), "hops": w_s}
            for out_nbr in self.neighbors_cost_n.keys():
                await self.adapter.send(node_to_addr(out_nbr), msg)


    # --------------------------- manejo mensajes -------------------------

    async def _handle_msg(self, msg: Dict[str, Any], raw: Any):
        mtype = msg.get("type")
        src_addr = msg.get("from")
        dst_addr = msg.get("to")

        sender_addr = None
        if isinstance(raw, dict) and "sender_id" in raw:
            sender_addr = raw["sender_id"]

        src_node = addr_to_node(src_addr) if src_addr else None
        dst_node = addr_to_node(dst_addr) if dst_addr else None

        # HELLO: solo resetea TTL para vecinos directos
        if mtype == "hello" and src_node:
            if src_node in self.neighbors_cost_n:
                # asegura aristas locales con peso LOCAL (no el remoto)
                w_local = float(self.neighbors_cost_n[src_node])
                self.graph.setdefault(self.node_id, {}).setdefault(src_node, {"weight": w_local, "time": self.neighbor_ttl_default})
                self.graph.setdefault(src_node, {}).setdefault(self.node_id, {"weight": w_local})
                # reset TTL
                self.graph[self.node_id][src_node]["time"] = self.neighbor_ttl_default
                if self.verbose:
                    print(f"[{self.me_addr}] HELLO de {src_addr} -> TTL reset {self.node_id}->{src_node}")
                self._recompute_spf()
                self._touch(src_node)
                self._touch(self.node_id)
            return

        # MESSAGE: (src --hops--> dst) aprendido por flooding
        if mtype == "message" and src_node and dst_node:
            w_in = _safe_weight(msg.get("hops"), default=None)
            if w_in is None:
                if self.verbose:
                    print(f"[{self.me_addr}] (ignorado) 'message' con hops inválido: {msg.get('hops')}")
                return
            changed = False

            def set_edge(u: str, v: str, w: float, preserve_ttl: bool = False):
                nonlocal changed
                self.graph.setdefault(u, {})
                cur = self.graph[u].get(v, {})
                cur_w = cur.get("weight")
                if cur_w != w:
                    if preserve_ttl and "time" in cur:
                        # conserva TTL local
                        t = cur["time"]
                        self.graph[u][v] = {"weight": w, "time": t}
                    else:
                        self.graph[u][v] = {"weight": w}
                    changed = True

            involves_me = (src_node == self.node_id) or (dst_node == self.node_id)
            
            if involves_me:
                # Si el borde recibido me involucra, NO confíes en w_in, usa mi peso local
                if src_node == self.node_id:
                    w_local = float(self.neighbors_cost_n.get(dst_node, w_in))
                    set_edge(self.node_id, dst_node, w_local, preserve_ttl=True)
                    set_edge(dst_node, self.node_id, w_local, preserve_ttl=False)
                elif dst_node == self.node_id:
                    w_local = float(self.neighbors_cost_n.get(src_node, w_in))
                    set_edge(self.node_id, src_node, w_local, preserve_ttl=True)
                    set_edge(src_node, self.node_id, w_local, preserve_ttl=False)
            else:
                # Borde remoto (no me involucra): usa w_in tal cual (grafo no dirigido)
                set_edge(src_node, dst_node, w_in)
                set_edge(dst_node, src_node, w_in)
                
            self._touch(src_node)
            self._touch(dst_node)

            if changed:
                if self.verbose:
                    print(f"[{self.me_addr}] learn: {src_node}<->{dst_node} w={w_in}")
                self._recompute_spf()
                # Flood controlado: reenvía a mis vecinos excepto el que lo publicó
                for out_nbr in self.neighbors_cost_n.keys():
                    out_addr = node_to_addr(out_nbr)
                    if sender_addr and out_addr == sender_addr:
                        continue
                    await self.adapter.send(out_addr, msg)
            return

        # Otros: ignora

    
        
    # ---------------------------- Dijkstra (SPF) -------------------------

    def _recompute_spf(self):
        """Dijkstra desde self.node_id sobre self.graph."""
        G: Dict[str, Dict[str, float]] = {}
        for u, nbrs in self.graph.items():
            G.setdefault(u, {})
            for v, d in nbrs.items():
                w = float(d.get("weight", 1.0))
                G[u][v] = w

        src = self.node_id
        nodes = set([src]) | set(G.keys())
        for u in G:
            nodes |= set(G[u].keys())
        nodes = list(nodes)

        dist = {n: math.inf for n in nodes}
        prev = {n: None for n in nodes}
        dist[src] = 0.0

        pq: list[tuple[float, str]] = [(0.0, src)]
        seen = set()
        while pq:
            d, u = heapq.heappop(pq)
            if u in seen:
                continue
            seen.add(u)
            for v, w in G.get(u, {}).items():
                nd = d + w
                if nd < dist.get(v, math.inf):
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        new_rt: Dict[str, tuple[str, float]] = {}
        for dst in nodes:
            if dst == src or dist.get(dst, math.inf) == math.inf:
                continue
            # reconstruir path
            path = []
            cur = dst
            while cur is not None:
                path.append(cur)
                cur = prev.get(cur)
            path.reverse()
            if len(path) >= 2:
                new_rt[dst] = (path[1], dist[dst])

        self.routes = new_rt
        if self.verbose:
            pretty = ", ".join(f"{d}->({nh},{c:.0f})" for d,(nh,c) in sorted(self.routes.items()))
            print(f"[{self.me_addr}] rutas: {pretty if pretty else '{}'}")

    # ---------------------------- UI helpers -----------------------------

    def _fmt_cost(self, x):
        try:
            x = float(x)
            return int(x) if x.is_integer() else round(x, 3)
        except Exception:
            return x

    def lsdb_snapshot(self) -> dict[str, dict[str, float]]:
        """
        LSDB consolidada = enlaces locales vivos (TTL>0) + todo lo aprendido por flooding.
        """
        snap: dict[str, dict[str, float]] = {}
        # enlaces locales vivos
        me = self.node_id
        row = self.graph.get(me, {})
        for nb, d in row.items():
            if nb == me: 
                continue
            if d.get("time", 0) > 0:
                snap.setdefault(me, {})[nb] = float(d["weight"])
        # resto del grafo (sin TTL)
        for u, nbrs in self.graph.items():
            for v, d in nbrs.items():
                if u == me and v in row and row[v].get("time", 0) > 0:
                    # ya lo pusimos arriba
                    continue
                w = float(d.get("weight", 1.0))
                snap.setdefault(u, {})[v] = min(w, float(snap.get(u, {}).get(v, float("inf"))))
        return snap

    def print_routes(self):
        if not self.routes:
            print("(vacio) aún no hay rutas calculadas")
            return
        src = self.node_id
        print("Ruta      : Costo")
        print("------------------")
        for dst in sorted(self.routes.keys()):
            nh, c = self.routes[dst]
            c2 = int(c) if float(c).is_integer() else round(float(c), 3)
            print(f"{src} -> {dst} : {c2} (nh={nh})")


    def print_table(self):
        # imprime todas las aristas conocidas
        lines = []
        for u in sorted(self.graph.keys()):
            for v in sorted(self.graph[u].keys()):
                w = self.graph[u][v].get("weight", 1.0)
                t = self.graph[u][v].get("time", None)
                if t is None:
                    lines.append(f"{u} -> {v}  w={self._fmt_cost(w)}")
                else:
                    lines.append(f"{u} -> {v}  w={self._fmt_cost(w)}  t={int(t)}")
        if lines:
            print("\n".join(lines))
        else:
            print("(tabla vacía)")

    def print_lsdb(self):
        snap = self.lsdb_snapshot()
        print("[LSDB]")
        for u in sorted(snap.keys()):
            items = ", ".join(f"{v}:{self._fmt_cost(w)}" for v, w in sorted(snap[u].items()))
            print(f"  {u} -> {{ {items} }}")

    def _touch(self, node: str):
        try:
            self.last_seen[node] = asyncio.get_event_loop().time()
        except RuntimeError:
            # por si no hay loop aún (arranque)
            self.last_seen[node] = time.time()
            
    def _purge_node_everywhere(self, node: str) -> bool:
        changed = False
        # borra fila completa
        if node in self.graph:
            del self.graph[node]
            changed = True
        # borra referencias en otras filas
        for u in list(self.graph.keys()):
            if node in self.graph[u]:
                del self.graph[u][node]
                changed = True
        return changed