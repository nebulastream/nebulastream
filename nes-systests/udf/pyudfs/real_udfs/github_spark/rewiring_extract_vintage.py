# codon-fuzz: entry=extract_vintage args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["<1940"], ["1940s"], ["1960s"], ["1980s"], ["2000s"], ["2010s "]]
# real-udf: origin=github-spark id=rewiring_extract_vintage requires= wrapped=0 ported=0
# Source: https://github.com/rewiringamerica/surrogate_modeling/blob/8e5900d9f9f4f4ec695572560f1ef7ff0811d80a/src/feature_utils.py#L172-L184
# Repository: rewiringamerica/surrogate_modeling @ commit 8e5900d9f9f4f4ec695572560f1ef7ff0811d80a
# License: Apache-2.0
# Copied verbatim from src/feature_utils.py (L172-L184); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=IntegerType())

def extract_vintage(vintage: str) -> int:
    """Return the midpoint of the vintage decade bin, with '<1940' as 1930.

    >>> extract_vintage('<1940')
    1930
    >>> extract_vintage('1960s')
    1965
    """
    vintage = vintage.strip()
    if vintage.startswith("<"):  # '<1940' bin in resstock
        return 1930
    return int(vintage[:4]) + 5
