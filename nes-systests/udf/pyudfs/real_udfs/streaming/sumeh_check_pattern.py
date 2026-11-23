# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["john.doe@example.com", "^[\\w.+-]+@[\\w-]+\\.[\\w.]+$"], ["john.doe@example", "^[\\w.+-]+@[\\w-]+\\.[\\w.]+$"], ["+49 30 1234567", "^\\+?[0-9 ]{7,15}$"], ["2024-05-20", "^\\d{4}-\\d{2}-\\d{2}$"], ["20/05/2024", "^\\d{4}-\\d{2}-\\d{2}$"], ["10115", "\\d{5}"], ["SW1A 1AA", "\\d{5}"]]
# real-udf: origin=streaming id=sumeh_check_pattern requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) and @stream_validator decorators; @udf only registers the function with PyFlink, @stream_validator (sumeh) only calls the function and sets a marker attribute.
# Wrapper: the validator returns None when the value passes; case_entry returns "" for None.
# Imports: the original module's imports that this code uses.

import re

# Source: https://github.com/maltzsama/sumeh/blob/da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516/sumeh/engines/pyflink/udfs.py#L194-L202
# Repository: https://github.com/maltzsama/sumeh (commit da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def sumeh_check_pattern(value, pattern):
    """Check if value matches regex pattern."""
    if value is None:
        return None
    if not re.match(pattern, str(value)):
        return f"pattern_mismatch:{pattern}"
    return None


def case_entry(value, pattern):
    result = sumeh_check_pattern(value, pattern)
    return "" if result is None else result
