# codon-fuzz: entry=udf args=FLOAT64 returns=VARSIZED
def udf(x):
    out = []
    try:
        out.append(str(int(x)))
    except OverflowError:
        out.append("ovf")
    except ValueError:
        out.append("val")
    out.append(str(float(int(x) if abs(x) < 1e18 else 0) == x))
    return ",".join(out)
