# codon-fuzz: entry=udf args=VARSIZED,FLOAT64 returns=VARSIZED


def udf(s, x):
    return f"{x:5.1f}%"
