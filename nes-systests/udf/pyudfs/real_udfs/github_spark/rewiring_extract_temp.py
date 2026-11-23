# codon-fuzz: entry=extract_temp args=VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["70F"], ["68F"], ["72F"], ["60F"], ["55F "], ["78F"]]
# real-udf: origin=github-spark id=rewiring_extract_temp requires= wrapped=0 ported=0
# Source: https://github.com/rewiringamerica/surrogate_modeling/blob/8e5900d9f9f4f4ec695572560f1ef7ff0811d80a/src/feature_utils.py#L198-L209
# Repository: rewiringamerica/surrogate_modeling @ commit 8e5900d9f9f4f4ec695572560f1ef7ff0811d80a
# License: Apache-2.0
# Copied verbatim from src/feature_utils.py (L198-L209); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=DoubleType())

import re


def extract_temp(temperature_string) -> float:
    """Convert string Fahrenheit degrees to float F.

    >>> extract_temp('70F')
    70.0
    >>> extract_temp('-3F')
    -3.0
    """
    if not re.match(r"\d+F", temperature_string):
        raise ValueError(f"Unrecognized temperature format: {temperature_string}")
    return float(temperature_string.strip().lower()[:-1])
