# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
def udf(n):
    m = n % 9 - 4
    try:
        p = 2 ** m
        r = str(p)
    except ZeroDivisionError:
        r = "z"
    try:
        r += "|" + str(pow(3, m, 7))
    except ValueError:
        r += "|v"
    r += "|" + str((-8) ** (1 / 3) if False else (-2) ** 3)
    r += "|" + str(m ** 0)
    return r
