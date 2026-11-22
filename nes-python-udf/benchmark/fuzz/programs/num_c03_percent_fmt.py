# codon-fuzz: entry=udf args=FLOAT64,INT64 returns=VARSIZED
def udf(x, n):
    p = n % 8
    c = format(x, "." + str(p) + "f") if abs(x) < 1e20 else format(x, "f")
    d = format(x, "10.3g") + "|" + format(x, "<12.2e") + "|" + format(x, "+.1f")
    e = "{:.{}g}".format(x, p + 1)
    f = f"{x:#g}|{x:_.1f}"
    return "|".join([c, d, e, f])
