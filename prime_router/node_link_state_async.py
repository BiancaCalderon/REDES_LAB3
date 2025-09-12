import asyncio, uuid, time, math, heapq, json
from redis_adapter_async import RedisAdapterAsync

HELLO_INTERVAL = 3       # cada 3s envío hello a vecinos directos
NEIGHBOR_TTL   = 30      # 10 hellos ~ 30s: liveness para vecinos directos
REMOTE_TTL     = 15      # edad máxima de enlaces remotos
DEBOUNCE_SEC   = 0.5     # anti-rebote en recompute

def _norm_type(t: str) -> str:
    if not t: return ""
    t = t.lower()
    if t in ("message", "edge", "link"): return "message"
    if t in ("hello", "keepalive", "ping"): return "hello"
    if t in ("data", "payload"): return "data"
    return t

def _norm_edge_keys(d: dict):
    """Devuelve (u,v,w) normalizando nombres usados por otros grupos."""
    u = d.get("from") or d.get("src") or d.get("source")
    v = d.get("to")   or d.get("dst") or d.get("dest")
    w = d.get("hops") or d.get("weight") or d.get("cost") or 1
    try: w = float(w)
    except: w = 1.0
    return u, v, w

class NodeLSAsync:
    def __init__(self, node_id: str, neighbors: dict, host, port, password):
        self.node_id = node_id
        self.neighbors = neighbors.copy()  # {vecino: costo}
        self.adapter = RedisAdapterAsync(node_id, host=host, port=port, password=password)

        # Topología interna: nodo -> { vecino: {weight, time} }
        # Yo -> vecino: conozco el peso y considero el enlace "configurado".
        # Vecino -> yo: NO lo marco vivo hasta recibir hello (time=0).
        self.topology = {
            node_id: {nb: {"weight": w, "time": NEIGHBOR_TTL} for nb, w in self.neighbors.items()}
        }
        for nb, w in self.neighbors.items():
            self.topology.setdefault(nb, {})
            # ¡aquí el fix! empieza en 0: no está vivo hasta que diga "hello".
            self.topology[nb][node_id] = {"weight": w, "time": 0}

        self.seen = set()
        self.routing_table = {}
        self.verbose = False
        self._pending_recompute = False

    # ---------- ciclo de vida ----------
    async def start(self):
        async def on_message(msg):
            # Mostrar cosas raras si vienen en texto plano
            if not isinstance(msg, dict):
                if self.verbose:
                    print(f"[{self.node_id}] RAW ← {msg!r}")
                return

            mid = msg.get("message_id")
            if mid in self.seen:
                return
            self.seen.add(mid)

            t = _norm_type(msg.get("type"))

            if t == "hello":
                u, v, w = _norm_edge_keys(msg)
                if not u or not v: return
                self._handle_hello(u, v, w)
                if self.verbose:
                    print(f"[{self.node_id}] ← HELLO {u}→{v} w={w}")
                return

            if t == "message":
                u, v, w = _norm_edge_keys(msg)
                if not u or not v: return
                changed = self._handle_edge(u, v, w)
                if self.verbose and changed:
                    print(f"[{self.node_id}] ← EDGE {u}→{v} w={w} (NEW/CHANGED)")
                if changed:
                    await self._flood(msg, exclude=msg.get("from"))
                await self._schedule_recompute()
                return

            if t == "data":
                if msg.get("to") == self.node_id:
                    print(f"[{self.node_id}] 🎯 DATA de {msg.get('from')}: {msg.get('payload')}")
                else:
                    await self.forward_data(msg)
                return

            # tipos desconocidos -> ignora
            if self.verbose:
                print(f"[{self.node_id}] (ignorado) tipo={t} msg={msg}")

        await self.adapter.start(on_message)
        await self._announce_my_links()
        asyncio.create_task(self._hello_loop())
        asyncio.create_task(self._timer_loop())
        print(f"[{self.node_id}] Nodo LSR (spec) iniciado. Vecinos: {list(self.neighbors.keys())}")

    # ---------- handlers ----------
    def _handle_hello(self, u, v, w):
        # reseteo liveness solo si el hello es "hacia mí"
        if v != self.node_id:
            return
        # yo->u ya existe; aquí confirmo u->yo y reseteo TTL
        self.topology.setdefault(self.node_id, {})
        self.topology.setdefault(u, {})
        self.topology[self.node_id].setdefault(u, {"weight": w, "time": NEIGHBOR_TTL})
        self.topology[u][self.node_id] = {"weight": w, "time": NEIGHBOR_TTL}

    def _handle_edge(self, u, v, w) -> bool:
        self.topology.setdefault(u, {})
        cur = self.topology[u].get(v)
        if cur is None or cur["weight"] != w:
            self.topology[u][v] = {"weight": w, "time": REMOTE_TTL}
            return True
        cur["time"] = REMOTE_TTL
        return False

    # ---------- envío ----------
    async def _hello_loop(self):
        while True:
            await asyncio.sleep(HELLO_INTERVAL)
            for nb, w in self.neighbors.items():
                hello = {"type":"hello","from":self.node_id,"to":nb,"hops":w,"message_id":str(uuid.uuid4())[:8]}
                await self.adapter.send(nb, hello)
                if self.verbose:
                    print(f"[{self.node_id}] → HELLO {nb} (hops={w})")

    async def _announce_my_links(self):
        # Propago mis enlaces (yo→vecino) para que la red vaya aprendiendo
        for nb, w in self.neighbors.items():
            m = {"type":"message","from":self.node_id,"to":nb,"hops":w,"message_id":str(uuid.uuid4())[:8]}
            self.seen.add(m["message_id"])
            await self._flood(m)
            if self.verbose:
                print(f"[{self.node_id}] → EDGE announce {self.node_id}→{nb} w={w}")

    async def _flood(self, msg, exclude=None):
        for nb in self.neighbors.keys():
            if nb == exclude: 
                continue
            await self.adapter.send(nb, msg)

    # ---------- timers / expiración ----------
    async def _timer_loop(self):
        while True:
            await asyncio.sleep(HELLO_INTERVAL)
            dirty = False
            for u in list(self.topology.keys()):
                for v in list(self.topology[u].keys()):
                    self.topology[u][v]["time"] -= HELLO_INTERVAL
                    if self.topology[u][v]["time"] <= 0:
                        # expira enlace
                        if self.verbose:
                            print(f"[{self.node_id}] 🔻 expira {u}→{v}")
                        del self.topology[u][v]; dirty = True
                if u in self.topology and not self.topology[u]:
                    del self.topology[u]; dirty = True
            if dirty:
                await self._schedule_recompute()

    async def _schedule_recompute(self):
        if self._pending_recompute: 
            return
        self._pending_recompute = True
        await asyncio.sleep(DEBOUNCE_SEC)
        self._recompute_routes()
        self._pending_recompute = False

    # ---------- SPF ----------
    def _graph_now(self):
        g = {}
        for u, adj in self.topology.items():
            for v, meta in adj.items():
                if meta["time"] > 0:
                    g.setdefault(u, {})[v] = float(meta["weight"])
        return g

    def _recompute_routes(self):
        g = self._graph_now()
        nodes = set(g.keys())
        for u in g:
            nodes.update(g[u].keys())
        nodes = list(nodes)
        if self.node_id not in nodes:
            nodes.append(self.node_id)

        dist = {n: math.inf for n in nodes}
        prev = {n: None for n in nodes}
        dist[self.node_id] = 0.0

        pq = [(0.0, self.node_id)]
        seen = set()
        while pq:
            d,u = heapq.heappop(pq)
            if u in seen: 
                continue
            seen.add(u)
            for v,w in g.get(u, {}).items():
                nd = d + w
                if nd < dist.get(v, math.inf):
                    dist[v] = nd; prev[v] = u
                    heapq.heappush(pq, (nd,v))

        rt = {}
        for dst in nodes:
            if dst == self.node_id or dist.get(dst, math.inf) == math.inf:
                continue
            path = []; cur = dst
            while cur is not None:
                path.append(cur); cur = prev.get(cur)
            path.reverse()
            if len(path) > 1:
                rt[dst] = (path[1], dist[dst])

        self.routing_table = rt
        if self.verbose:
            print(f"[{self.node_id}] rutas: {self.routing_table}")

    # ---------- DATA de prueba ----------
    async def forward_data(self, msg: dict):
        dest = msg.get("to")
        if dest in self.neighbors:
            msg["ttl"] = msg.get("ttl", 16) - 1
            if msg["ttl"] > 0:
                await self.adapter.send(dest, msg)
                print(f"[{self.node_id}] ▶ DATA (vecino) → {dest}")
            return
        if dest not in self.routing_table:
            print(f"[{self.node_id}] ⚠️ DATA sin ruta hacia {dest}")
            return
        next_hop,_ = self.routing_table[dest]
        msg["ttl"] = msg.get("ttl", 16) - 1
        if msg["ttl"] > 0:
            await self.adapter.send(next_hop, msg)
            print(f"[{self.node_id}] ▶ DATA {dest} via {next_hop}")

    async def send_data(self, dest: str, payload: str):
        msg = {"type":"data","from":self.node_id,"to":dest,"ttl":16,
               "payload":payload,"message_id":str(uuid.uuid4())[:8]}
        self.seen.add(msg["message_id"])
        await self.forward_data(msg)

    # ---------- utilidad: estado de vecinos ----------
    def neighbors_status(self):
        """UP si (vecino -> yo) tiene TTL>0. DOWN si nunca recibí hello reciente."""
        status = {}
        for nb in self.neighbors.keys():
            t = self.topology.get(nb, {}).get(self.node_id, {"time":0})["time"]
            status[nb] = {"alive": t > 0, "remaining": t}
        return status
