# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["shipped", "['pending', 'shipped', 'delivered']"], ["cancelled", "['pending', 'shipped', 'delivered']"], ["DE", "['DE', 'AT', 'CH']"], ["FR", "['DE', 'AT', 'CH']"], ["visa", "['visa', 'mastercard', 'amex']"], ["paypal", "('visa', 'mastercard')"]]
# real-udf: origin=streaming id=sumeh_check_in_list requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) and @stream_validator decorators; @udf only registers the function with PyFlink, @stream_validator (sumeh) only calls the function and sets a marker attribute.
# Wrapper: the validator returns None when the value passes; case_entry returns "" for None.

# Source: https://github.com/maltzsama/sumeh/blob/da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516/sumeh/engines/pyflink/udfs.py#L164-L174
# Repository: https://github.com/maltzsama/sumeh (commit da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def sumeh_check_in_list(value, allowed_values):
    """Check if value in allowed list."""
    if value is None:
        return None
    # allowed_values comes as string, parse it
    allowed = eval(allowed_values)  # Convert "['a','b']" to list
    if value not in allowed:
        return f"not_in_list:{allowed_values}"
    return None


def case_entry(value, allowed_values):
    result = sumeh_check_in_list(value, allowed_values)
    return "" if result is None else result
