# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
def udf(s, n):
    a = list(s[:12])
    k = n % 5
    try:
        a[::-2] = ["#"] * len(a[::-2])
    except ValueError:
        a.append("E")
    b = list(range(8))
    b[k:k] = [77, 78]
    b[-1:-4:-1] = [1, 2, 3]
    b[5:2] = [9]
    try:
        b[::2] = [0]
    except ValueError:
        b.append(-1)
    c = list(range(6))
    del c[-2::-2]
    d = list(range(5))
    d[1:3] = []
    d[len(d):] = [8, 9]
    return "".join(a) + str(b) + str(c) + str(d) + str(b[k::-1]) + str(b[-100:100:3])
