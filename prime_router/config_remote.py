# config_remote.py
HOST = "homelab.fortiguate.com"
PORT = 16379
PWD  = "4YNydkHFPcayvlx7$zpKm"

def node_to_addr(nstr: str, prefix="grupo") -> str:
    # "N8" -> "sec30.grupo8.nodo8"
    n = int(nstr.replace("N", ""))
    return f"sec30.{prefix}{n}.nodo{n}"

def addr_to_node(addr: str) -> str:
    # "sec30.grupo4.nodo4" -> "N4"
    try:
        last = addr.split(".")[-1]   # "nodo4"
        n = int(last.replace("nodo", ""))
        return f"N{n}"
    except Exception:
        return addr  # por si ya viene como "N#"
