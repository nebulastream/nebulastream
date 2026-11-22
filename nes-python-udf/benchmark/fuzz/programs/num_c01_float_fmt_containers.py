# codon-fuzz: entry=udf args=FLOAT64,INT64 returns=VARSIZED
def udf(x, n):
    vals = [x, x / 3, float(n), x * 10]
    pair = (x, n % 7)
    d = {"x": x, "half": x / 2}
    return str(vals) + "|" + str(pair) + "|" + str(d) + "|" + repr(x) + "|" + str({x})
