# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
def windows(seq, k):
    buf = []
    for item in seq:
        buf.append(item)
        if len(buf) > k:
            buf.pop(0)
        if len(buf) == k:
            yield "".join(buf)

def take(gen, m):
    out = []
    for i, v in enumerate(gen):
        if i >= m:
            break
        out.append(v)
    return out

def udf(s, n):
    k = n % 4 + 1
    ws = take(windows(s, k), 5)
    acc = []
    g = (ord(c) % 10 for c in s)
    total = sum(g)
    acc.append(str(total))
    acc.append(str(ws))
    acc.append(str(list(zip(s, range(3)))))
    acc.append(str(any(c.isdigit() for c in s)))
    return "|".join(acc)
