
from config_remote import chanN

MY = chanN(8)  # sec30.grupo8.nodo8


NEIGHBORS_FLOOD = [
    chanN(7), chanN(3), chanN(4), chanN(10), chanN(11)
]


NEIGHBORS_COST = {
    chanN(7): 7,
    chanN(3): 15,
    chanN(4): 8,
    chanN(10): 6,
    chanN(11): 16,
}
