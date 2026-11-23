# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["2024-05-20"], ["2024-05-25"], ["2024-05-26"], ["2023-12-29"], ["2000-01-01"], ["2026-11-23"]]
# real-udf: origin=streaming id=sumeh_check_date_weekday requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) and @stream_validator decorators; @udf only registers the function with PyFlink, @stream_validator (sumeh) only calls the function and sets a marker attribute.
# Wrapper: the validator returns None when the value passes; case_entry returns "" for None.
# Wrapper: PyFlink passes a DATE column as datetime.date; case_entry builds it from ISO text with date.fromisoformat.
# Imports: the original module's imports that this code uses.

from datetime import datetime, date, timedelta

# Source: https://github.com/maltzsama/sumeh/blob/da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516/sumeh/engines/pyflink/udfs.py#L314-L326
# Repository: https://github.com/maltzsama/sumeh (commit da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def sumeh_check_date_weekday(value):
    """Check if date is on weekday (Mon-Fri)."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date.weekday() >= 5:  # 5=Sat, 6=Sun
            return "not_weekday"
    except:
        return "invalid_date"
    return None


def case_entry(value):
    result = sumeh_check_date_weekday(date.fromisoformat(value))
    return "" if result is None else result
