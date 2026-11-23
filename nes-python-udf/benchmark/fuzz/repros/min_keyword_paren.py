# codon-fuzz: entry=udf args=INT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[3], [0], [7]]
# Before Codon fork 3b8c3a9+, the token parser rejected a keyword directly followed by '(' (found in real UDFs:
# `if(match):`, `return((True, dist))`, `raise(e)`, and `while(...)` in CPython's difflib).


def udf(n):
    d = {1: 2}
    del(d[1])
    assert(n >= 0)
    if(n > 5):
        return(2)
    elif(n in(1, 2, 3)):
        return(1 if not(n == 2) else 4)
    while(n > 0):
        n -= 1
    try:
        raise(ValueError("x"))
    except ValueError:
        return(len(d) - 1)
