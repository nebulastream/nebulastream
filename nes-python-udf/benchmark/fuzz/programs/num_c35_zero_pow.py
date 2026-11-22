# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
def udf(n):
    b = n % 3 - 1
    try:
        return str(b ** -2)
    except ZeroDivisionError:
        return "zde"
