# codon-fuzz: entry=udf args=INT64,FLOAT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[7, 1234.5], [-42, -0.25], [1234567, 98765.4321], [0, 0.0]]
# Before Codon fork dd19436+ and NES 8e7a7705c1+, {n:06d} padded with spaces, {n:,} and {x:,.2f} had no grouping, and a
# spec starting with a sign ({n:+d}, {x:+.2f}, {n: d}) was a syntax error. Not covered here: "_" grouping, the "%" type and a
# precision without a type ({x:10.4}), which differ.


def udf(n, x):
    return f"{n:06d}|{n:,}|{x:,.2f}|{n:+d}|{x:+08.3f}|{n:>8,}|{x:.1f}|{n:#x}|{x:.3e}|{n: d}|{n:^9d}"
