# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["active", "active"], ["inactive", "active"], ["EUR", "USD"], ["DE", "DE"], ["de", "DE"]]
# real-udf: origin=streaming id=sumeh_check_equal requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) and @stream_validator decorators; @udf only registers the function with PyFlink, @stream_validator (sumeh) only calls the function and sets a marker attribute.
# Wrapper: the validator returns None when the value passes; case_entry returns "" for None.

# Source: https://github.com/maltzsama/sumeh/blob/da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516/sumeh/engines/pyflink/udfs.py#L34-L42
# Repository: https://github.com/maltzsama/sumeh (commit da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def sumeh_check_equal(value, expected):
    """Check if value == expected."""
    if value is None:
        return None
    if value != expected:
        return f"not_equal:{expected}"
    return None


def case_entry(value, expected):
    result = sumeh_check_equal(value, expected)
    return "" if result is None else result
