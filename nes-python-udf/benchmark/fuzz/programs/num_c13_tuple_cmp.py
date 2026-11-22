# codon-fuzz: entry=udf args=VARSIZED,FLOAT64 returns=VARSIZED
def udf(s, x):
    a = (len(s), x, s)
    b = (len(s), 0.0, "m")
    c = (len(s), float("nan"), s)
    res = [a < b, a == b, a <= b, c == c, c < a, (x,) == (x,), [x] == [x], x == x]
    return str(res) + max(a, b)[2]
