# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[5], [-3], [99999]]
class AddressError(ValueError):
    pass


class _AddressBase:
    def check(self, address):
        if address < 0:
            raise AddressError("%d (< 0) is not permitted as an IPv%d address" % (address, self.version))
        if address > self._ALL_ONES:
            raise AddressError("%d (>= 2**%d) is not permitted as an IPv%d address" % (address, self.bits, self.version))
        return "ok v%d" % self.version

    def describe(self, address):
        return "[" + self.check(address) + "]"


class _V4:
    version = 4
    bits = 8
    _ALL_ONES = (1 << 8) - 1


class _V6:
    version = 6
    bits = 16
    _ALL_ONES = (1 << 16) - 1


class V4Address(_V4, _AddressBase):
    pass


class V6Address(_V6, _AddressBase):
    pass


def attempt(f, a):
    try:
        return f(a)
    except AddressError as e:
        return "AddressError:" + str(e)


def udf(n):
    return attempt(lambda a: V4Address().describe(a), n) + "|" + attempt(lambda a: V6Address().describe(a), n)
