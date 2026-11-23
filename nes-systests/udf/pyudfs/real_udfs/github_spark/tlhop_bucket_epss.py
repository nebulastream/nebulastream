# codon-fuzz: entry=bucket_epss args=FLOAT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0.04], [19.99], [20.0], [45.5], [61.2], [79.9], [97.3]]
# real-udf: origin=github-spark id=tlhop_bucket_epss requires= wrapped=0 ported=0
# Source: https://github.com/lucasmsp/tlhop-library/blob/c2943f775bbc9bc99705d5db2495dea3f6528fe3/tlhop/library.py#L849-L864
# Repository: lucasmsp/tlhop-library @ commit c2943f775bbc9bc99705d5db2495dea3f6528fe3
# License: Apache-2.0
# Copied verbatim from tlhop/library.py (L849-L864); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf

def bucket_epss(score):
    """
    EPSS Score, 0 to 100%.
    """

    if score < 20:
        return "< 20"
    elif score < 40:
        return "< 40"
    elif score < 60:
        return "< 60"
    elif score < 80:
        return "< 80"
    else:
        return ">= 80"
