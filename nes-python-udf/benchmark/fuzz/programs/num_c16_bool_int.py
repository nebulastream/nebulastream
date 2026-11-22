# codon-fuzz: entry=udf args=INT64,BOOLEAN returns=VARSIZED
def udf(n, b):
    m = n % 100
    r = []
    r.append(str(int(b) * m))
    r.append(str(m // (int(b) + 1)))
    r.append(str(b) + str(int(b)))
    r.append(str(sum([b, True, m > 3])))
    r.append(str(b & (m > 2)))
    r.append(str(b ^ True))
    r.append(str(b | False))
    r.append(str(max(b, False)))
    r.append(str(bool(m) is b))
    r.append(str([m, -m][b]))
    r.append(str(b == 1) + str(b is True) + str(True == 1.0))
    return ",".join(r)
