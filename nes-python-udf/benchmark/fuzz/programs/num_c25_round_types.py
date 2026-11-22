# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
def udf(n):
    a = round(n, 0)
    b = round(n, 2)
    c = round(n)
    return type(a).__name__ + type(b).__name__ + type(c).__name__ + "|" + str(a) + "|" + str(c)
