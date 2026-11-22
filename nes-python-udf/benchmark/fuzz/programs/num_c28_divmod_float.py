# codon-fuzz: entry=udf args=FLOAT64,INT64 returns=VARSIZED
def udf(x, n):
    m = n % 7 - 3
    try:
        q, r = divmod(x, float(m))
        res = f"{q:.4g}/{r:.4g}"
    except ZeroDivisionError:
        res = "z"
    try:
        res += "|" + f"{x % m:.4g}"
    except ZeroDivisionError:
        res += "|z"
    try:
        res += "|" + f"{m ** -1:.4g}"
    except ZeroDivisionError:
        res += "|z"
    try:
        res += "|" + f"{(0.0) ** m:.4g}"
    except ZeroDivisionError:
        res += "|z"
    return res
