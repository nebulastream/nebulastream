# codon-fuzz: entry=udf args=INT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[3], [0]]
# Codon (fork 3b8c3a9) crashes with SIGSEGV on the first row: 'x and float(x)' with an Optional x, then arithmetic.
# Found in the real UDF github_spark/rewiring_extract_percentage.


def udf(n):
    x = None if n == 0 else n
    return (x and float(x)) / 2.0
