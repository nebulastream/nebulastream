# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
def adder(i):
    def f(v, extra=0):
        return v + i + extra
    return f

def make_adders():
    fs = []
    for i in range(4):
        fs.append(adder(i))
    return fs

def counter():
    c = [0]
    def inc(step=1):
        c[0] += step
        return c[0]
    return inc

def accum(v, bucket=[0]):
    bucket.append(v)
    return len(bucket)

def udf(n):
    m = n % 100
    adds = [f(m) for f in make_adders()]
    inc = counter()
    inc(); inc(m)
    late = [lambda: k for k in range(3)]
    return str(adds) + ";" + str(inc()) + ";" + str([f() for f in late]) + ";" + str(accum(m))
