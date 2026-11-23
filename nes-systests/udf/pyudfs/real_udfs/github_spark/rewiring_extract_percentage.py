# codon-fuzz: entry=extract_percentage args=VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["100% Conditioned"], ["<10% Conditioned"], ["None"], ["10% Leakage, Uninsulated"], ["30% Leakage, R-6"], ["75% Conditioned"]]
# real-udf: origin=github-spark id=rewiring_extract_percentage requires= wrapped=0 ported=0
# Source: https://github.com/rewiringamerica/surrogate_modeling/blob/8e5900d9f9f4f4ec695572560f1ef7ff0811d80a/src/feature_utils.py#L150-L169
# Repository: rewiringamerica/surrogate_modeling @ commit 8e5900d9f9f4f4ec695572560f1ef7ff0811d80a
# License: Apache-2.0
# Copied verbatim from src/feature_utils.py (L150-L169); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=DoubleType())

import re


def extract_percentage(value: str) -> float:
    """Extract percentage from string and divide by 100.

    >>> extract_percentage('100% Conditioned')
    1.0
    >>> extract_percentage('<10% Conditioned')
    0.1
    >>> extract_percentage('None')
    0.0
    >>> extract_percentage('10% Leakage, Uninsulated')
    0.1
    """
    if value == "None":
        return 0.0
    match = re.match(r"^<?(\d+)%", value)
    try:
        return (match and float(match.group(1))) / 100.0
    except ValueError:
        raise ValueError(f"Cannot extract percentage from: f{value}")
