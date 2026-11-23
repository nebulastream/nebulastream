# codon-fuzz: entry=case_entry args=FLOAT64,FLOAT64,FLOAT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[42.0, 0.0, 100.0], [150.5, 0.0, 100.0], [-3.2, 0.0, 100.0], [18.0, 18.0, 65.0], [70.25, 18.0, 65.0], [0.15, 0.0, 0.1]]
# real-udf: origin=streaming id=sumeh_check_between requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) and @stream_validator decorators; @udf only registers the function with PyFlink, @stream_validator (sumeh) only calls the function and sets a marker attribute.
# Wrapper: the validator returns None when the value passes; case_entry returns "" for None.

# Source: https://github.com/maltzsama/sumeh/blob/da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516/sumeh/engines/pyflink/udfs.py#L148-L156
# Repository: https://github.com/maltzsama/sumeh (commit da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def sumeh_check_between(value, min_val, max_val):
    """Check if min_val <= value <= max_val."""
    if value is None:
        return None
    if value < min_val or value > max_val:
        return f"not_between:{min_val}-{max_val}"
    return None


def case_entry(value, min_val, max_val):
    result = sumeh_check_between(value, min_val, max_val)
    return "" if result is None else result
