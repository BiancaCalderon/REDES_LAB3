#!/usr/bin/env python3

import json
import os

def create_config_files():
    """Crea los archivos de configuración necesarios"""
    
    # Topología de ejemplo - red en diamante
    topology = {
        "type": "topo",
        "config": {
            "A": ["B", "C"],
            "B": ["A", "D"],
            "C": ["A", "D"], 
            "D": ["B", "C"]
        }
    }
    
    # Mapeo de nombres
    names = {
        "type": "names",
        "config": {
            "A": "nodeA@server.com",
            "B": "nodeB@server.com",
            "C": "nodeC@server.com", 
            "D": "nodeD@server.com"
        }
    }
    
    # Crear archivos
    with open("topo.txt", "w") as f:
        json.dump(topology, f, indent=2)
    
    with open("names.txt", "w") as f:
        json.dump(names, f, indent=2)
    
    print("Archivos de configuración creados:")
    print("- topo.txt: Topología de la red")
    print("- names.txt: Mapeo de nombres")
    print()
    print("Topología creada:")
    print("    A")
    print("   / \\")
    print("  B   C") 
    print("   \\ /")
    print("    D")
    print()
    print("Para probar:")
    print("Terminal 1: python dijkstra_router.py A")
    print("Terminal 2: python dijkstra_router.py B")
    print("Terminal 3: python dijkstra_router.py C") 
    print("Terminal 4: python dijkstra_router.py D")
    print()
    print("Luego en cualquier terminal:")
    print("A> send D Hola mundo")

if __name__ == "__main__":
    create_config_files()