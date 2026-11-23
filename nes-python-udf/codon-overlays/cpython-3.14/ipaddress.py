# Codon overlay for CPython 3.14's ipaddress (see ../README.md). Each definition replaces the one of the same name in
# the module; a class marked @__nes_patch__ replaces single methods.
#
# IPv4Network.__init__ and IPv6Network.__init__ replace the method `hosts` per object for networks of one or two
# addresses (`self.hosts = self.__iter__`, or a lambda). Compiled objects have fixed methods and fields of one type each,
# so here __init__ leaves `hosts` alone, and `hosts` tells these networks apart. The results are the same.


@__nes_patch__
class IPv4Network:
    def __init__(self, address, strict=True):
        addr, mask = self._split_addr_prefix(address)

        self.network_address = IPv4Address(addr)
        self.netmask, self._prefixlen = self._make_netmask(mask)
        packed = int(self.network_address)
        if packed & int(self.netmask) != packed:
            if strict:
                raise ValueError('%s has host bits set' % self)
            else:
                self.network_address = IPv4Address(packed &
                                                   int(self.netmask))

    def hosts(self):
        network = int(self.network_address)
        broadcast = int(self.broadcast_address)
        if self._prefixlen == (self.max_prefixlen - 1):
            first, last = network, broadcast + 1
        elif self._prefixlen == self.max_prefixlen:
            first, last = network, network + 1
        else:
            first, last = network + 1, broadcast
        for x in range(first, last):
            yield self._address_class(x)


@__nes_patch__
class IPv6Network:
    def __init__(self, address, strict=True):
        addr, mask = self._split_addr_prefix(address)

        self.network_address = IPv6Address(addr)
        self.netmask, self._prefixlen = self._make_netmask(mask)
        packed = int(self.network_address)
        if packed & int(self.netmask) != packed:
            if strict:
                raise ValueError('%s has host bits set' % self)
            else:
                self.network_address = IPv6Address(packed &
                                                   int(self.netmask))

    def hosts(self):
        network = int(self.network_address)
        broadcast = int(self.broadcast_address)
        if self._prefixlen == (self.max_prefixlen - 1):
            first, last = network, broadcast + 1
        elif self._prefixlen == self.max_prefixlen:
            first, last = network, network + 1
        else:
            first, last = network + 1, broadcast + 1
        for x in range(first, last):
            yield self._address_class(x)
