# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
def udf(s, n):
    a = list(range(10))
    k = n % 7
    a[k:k + 2] = [100, 101, 102]
    a[::3] = [-1] * len(a[::3])
    b = a[::-2]
    c = a[-2:2:-3]
    a.insert(-100, 55)
    a.insert(100, 66)
    a.remove(66)
    i = a.index(55)
    del a[1:4]
    del a[::4]
    words = s.split()
    words.sort(key=len, reverse=True)
    pairs = sorted([(len(w) % 3, w) for w in s.split()], key=lambda p: p[0])
    return str(a) + str(b) + str(c) + str(i) + str(words[:4]) + str(pairs[:4])
