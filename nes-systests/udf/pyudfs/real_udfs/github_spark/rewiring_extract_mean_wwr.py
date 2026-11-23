# codon-fuzz: entry=extract_mean_wwr args=VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["F9 B9 L9 R9"], ["F12 B12 L12 R12"], ["F15 B15 L15 R15"], ["F18 B9 L12 R6"], ["F30 B30 L30 R30"]]
# real-udf: origin=github-spark id=rewiring_extract_mean_wwr requires= wrapped=0 ported=0
# Source: https://github.com/rewiringamerica/surrogate_modeling/blob/8e5900d9f9f4f4ec695572560f1ef7ff0811d80a/src/feature_utils.py#L212-L230
# Repository: rewiringamerica/surrogate_modeling @ commit 8e5900d9f9f4f4ec695572560f1ef7ff0811d80a
# License: Apache-2.0
# Copied verbatim from src/feature_utils.py (L212-L230); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=DoubleType())

def extract_mean_wwr(value: str) -> int:
    """
    Return the average window to wall ratio (WWR) for front, back, left, and right walls.

    >>> extract_mean_wwr('F9 B9 L9 R9')
    9.0
    >>> extract_mean_wwr('F12 B12 L12 R12')
    12.0
    >>> extract_mean_wwr('Uninsulated')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError: Unrecognized format of window area: ...
    """
    try:
        wwr_list = [int(side[1:]) for side in value.split()]
        return sum(wwr_list) / len(wwr_list)
    except ValueError:
        raise ValueError(f"Unrecognized format of window area: {value}")
