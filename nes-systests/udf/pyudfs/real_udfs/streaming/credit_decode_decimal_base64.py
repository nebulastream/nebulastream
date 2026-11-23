# codon-fuzz: entry=decode_decimal_base64 args=VARSIZED,INT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["EtaH", 2], ["/C9w", 4], ["BfXg/w==", 2], ["BQ==", 0], ["IuyyXAA=", 6], ["/w==", 2], ["RKog", 2]]
# real-udf: origin=streaming id=credit_decode_decimal_base64 requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.
# Imports: the original module's imports.

import base64
from typing import Optional

# Source: https://github.com/ngnquanq/credit-risk-ml-system/blob/ff2442ed39d3f8b377e4aa52e668a494a5bd4b4b/application/flink/jobs/cdc_udfs.py#L16-L36
# Repository: https://github.com/ngnquanq/credit-risk-ml-system (commit ff2442ed39d3f8b377e4aa52e668a494a5bd4b4b)
# License: MIT
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def decode_decimal_base64(base64_str: str, scale: int) -> Optional[float]:
    """
    Decode Debezium decimal field from base64 string.

    Args:
        base64_str: Base64 encoded decimal value from Debezium
        scale: Number of decimal places

    Returns:
        Decoded decimal value as float, or None if decoding fails
    """
    if not base64_str or scale is None:
        return None
    try:
        raw = base64.b64decode(base64_str)
        # Two's complement big-endian integer
        unscaled = int.from_bytes(raw, byteorder="big", signed=True)
        return float(unscaled) / (10 ** int(scale))
    except Exception:
        return None
