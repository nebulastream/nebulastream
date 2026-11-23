from functools import cached_property


class Addr:
    def __init__(self, ip: int):
        self._ip = ip


class Net:
    _ALL_ONES = (1 << 32) - 1

    def __init__(self, ip: int, prefixlen: int):
        self.network = Addr(ip & ~((1 << (32 - prefixlen)) - 1))
        self.netmask = Addr(((1 << prefixlen) - 1) << (32 - prefixlen))

    @cached_property
    def hostmask(self):
        return Addr(self.netmask._ip ^ self._ALL_ONES)

    @cached_property
    def broadcast_address(self):
        return Addr(self.network._ip | self.hostmask._ip)


def base(ip: int) -> int:
    n = Net(ip, 24)
    return n.network._ip


def once(ip: int) -> int:
    n = Net(ip, 24)
    return n.broadcast_address._ip


def four(ip: int) -> int:
    n = Net(ip, 24)
    return (n.broadcast_address._ip + n.broadcast_address._ip + n.broadcast_address._ip + n.broadcast_address._ip)
