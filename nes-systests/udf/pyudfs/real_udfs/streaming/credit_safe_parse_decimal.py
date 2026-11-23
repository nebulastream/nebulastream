# codon-fuzz: entry=safe_parse_decimal args=VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1250.75"], ["-42.5"], ["1e3"], [" 3.14 "], ["0.0001"], ["250000"], ["1_000.5"]]
# real-udf: origin=streaming id=credit_safe_parse_decimal requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.

# Source: https://github.com/ngnquanq/credit-risk-ml-system/blob/ff2442ed39d3f8b377e4aa52e668a494a5bd4b4b/application/flink/jobs/cdc_udfs.py#L39-L58
# Repository: https://github.com/ngnquanq/credit-risk-ml-system (commit ff2442ed39d3f8b377e4aa52e668a494a5bd4b4b)
# License: MIT
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def safe_parse_decimal(value_str: str) -> Optional[float]:
    """
    Safely parse a decimal value that might be a number string or base64 encoded.

    Args:
        value_str: String representation of decimal value

    Returns:
        Parsed decimal value as float, or None if parsing fails
    """
    if not value_str:
        return None
    try:
        # Try direct float conversion first
        return float(value_str)
    except ValueError:
        # Could be base64 encoded, but without scale info we can't decode
        # In production, you'd need schema metadata to get the scale
        return None
