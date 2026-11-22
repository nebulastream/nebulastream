# codon-fuzz: entry=udf args=FLOAT64 returns=VARSIZED
def udf(x):
    vals = [1.0, x, 2.0]
    r = []
    r.append(str(x in vals))
    r.append(str(vals.count(x)))
    try:
        r.append(str(vals.index(x)))
    except ValueError:
        r.append("nf")
    r.append(str(vals == vals))
    r.append(str(vals == list(vals)))
    r.append(str((x, 1) == (x, 1)))
    return ",".join(r)
