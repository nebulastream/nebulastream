# codon-fuzz: entry=p_to_z args=FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0.05], [0.01], [0.5], [1e-08], [0.2], [0.999], [0.0001]]
# real-udf: origin=github-spark id=dig_p_to_z requires=scipy wrapped=0 ported=0
# Source: https://github.com/broadinstitute/dig-aggregator-methods/blob/eab455f691fe6d5bd2df6694b63ea840044f1324/huge/src/main/resources/huge-rare.py#L13-L15
# Repository: broadinstitute/dig-aggregator-methods @ commit eab455f691fe6d5bd2df6694b63ea840044f1324
# License: BSD-3-Clause
# Copied verbatim from huge/src/main/resources/huge-rare.py (L13-L15); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=DoubleType())

from scipy.stats import norm


def p_to_z(p_value: float) -> float:
    return float(abs(norm.ppf(p_value / 2.0)))
