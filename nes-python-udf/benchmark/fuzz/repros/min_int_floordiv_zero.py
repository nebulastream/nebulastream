# codon-fuzz: entry=udf args=INT64,INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[7, 0], [-9223372036854775808, -1], [-7, -1], [7, 2]]
# Before Codon fork 7164a15, int // 0 and % 0 with C numerics, and the smallest int // -1 in both modes, ended the
# process with SIGFPE. Now // 0 raises ZeroDivisionError as in CPython; the smallest int // -1 wraps (CPython: 2**63).


def udf(a, b):
    return str(a // b) + " " + str(a % b)
