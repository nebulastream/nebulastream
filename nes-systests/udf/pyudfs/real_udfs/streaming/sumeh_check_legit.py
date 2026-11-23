# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["john@example.com"], [""], ["   "], ["\t"], ["N/A"], ["0"]]
# real-udf: origin=streaming id=sumeh_check_legit requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) and @stream_validator decorators; @udf only registers the function with PyFlink, @stream_validator (sumeh) only calls the function and sets a marker attribute.
# Wrapper: the validator returns None when the value passes; case_entry returns "" for None.

# Source: https://github.com/maltzsama/sumeh/blob/da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516/sumeh/engines/pyflink/udfs.py#L205-L213
# Repository: https://github.com/maltzsama/sumeh (commit da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def sumeh_check_legit(value):
    """Check if value is not null and not empty string."""
    if value is None:
        return "null_value"
    if isinstance(value, str) and value.strip() == "":
        return "empty_string"
    return None


def case_entry(value):
    result = sumeh_check_legit(value)
    return "" if result is None else result
