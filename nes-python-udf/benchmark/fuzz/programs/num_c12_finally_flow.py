# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
def f(n):
    out = []
    for i in range(5):
        try:
            if (n + i) % 4 == 0:
                continue
            if (n + i) % 7 == 0:
                break
            if (n + i) % 5 == 0:
                raise ValueError("five")
            out.append(i)
        except ValueError:
            out.append(-i)
        else:
            out.append(100)
        finally:
            out.append(9)
    return out

def g(n):
    try:
        return n % 3
    finally:
        n = 99

def h(n):
    x = 0
    while True:
        try:
            x += 1
            if x > n % 6:
                return x
        finally:
            if x == 3:
                x += 10

def udf(n):
    return str(f(n)) + str(g(n)) + str(h(n))
