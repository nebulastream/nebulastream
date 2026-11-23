# codon-fuzz: entry=country_code_to_name args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["de"], ["US"], ["xs"], ["gb"], ["BR"], ["jp"], ["Xs"]]
# real-udf: origin=github-spark id=lucazugic_country_code_to_name requires=pycountry wrapped=0 ported=0
# Source: https://github.com/LucaZugic/local-pyspark-testing/blob/3e9db3bb7bb3c36424efb27f4e4f4cb425a98edf/src/local_pyspark_testing/transforms.py#L9-L18
# Repository: LucaZugic/local-pyspark-testing @ commit 3e9db3bb7bb3c36424efb27f4e4f4cb425a98edf
# License: MIT
# Copied verbatim from src/local_pyspark_testing/transforms.py (L5-L6, L9-L18); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

import pycountry


CUSTOM_CODES = {"XS": "Scousia"}
CUSTOM_NAMES = {"scousia": "XS"}


def country_code_to_name(alpha_2: str) -> str | None:
    """Convert ISO alpha-2 country code to full country name."""
    if not alpha_2:
        return None
    upper_code = alpha_2.upper()
    if upper_code in CUSTOM_CODES:
        return CUSTOM_CODES[upper_code]
    country = pycountry.countries.get(alpha_2=upper_code)
    return country.name if country else None
