# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED


def udf(s, n):
    c1 = chr(0xFFFF)
    c2 = chr(0x10000 + n % 1000)
    x = s + c1
    y = s + c2
    words = sorted([x, y, s, chr(0xE9), chr(0x65) + chr(0x301)])
    return str(x < y) + str(max(x, y) == y) + "|" + ",".join(str(len(w)) for w in words) + "|" + str(c2 in y) + str(y.find(c2)) + str(len(y))
