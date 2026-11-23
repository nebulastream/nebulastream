# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["2024-05-20", "2024-01-01", "2024-12-31"], ["2023-12-31", "2024-01-01", "2024-12-31"], ["2025-01-01", "2024-01-01", "2024-12-31"], ["2024-02-29", "2024-02-29", "2024-03-01"], ["2024-06-15", "2024-01-01", "2024-13-01"], ["2019-07-04", "2020-1-1", "2020-12-31"]]
# real-udf: origin=streaming id=sumeh_check_date_between requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) and @stream_validator decorators; @udf only registers the function with PyFlink, @stream_validator (sumeh) only calls the function and sets a marker attribute.
# Wrapper: the validator returns None when the value passes; case_entry returns "" for None.
# Wrapper: PyFlink passes a DATE column as datetime.date; case_entry builds it from ISO text with date.fromisoformat.
# Imports: the original module's imports that this code uses.

from datetime import datetime, date, timedelta

# Source: https://github.com/maltzsama/sumeh/blob/da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516/sumeh/engines/pyflink/udfs.py#L449-L463
# Repository: https://github.com/maltzsama/sumeh (commit da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def sumeh_check_date_between(value, start_str, end_str):
    """Check if date is between start and end."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_str, "%Y-%m-%d").date()
        if val_date < start or val_date > end:
            return f"not_between:{start_str}-{end_str}"
    except:
        return "invalid_date"
    return None


def case_entry(value, start_str, end_str):
    result = sumeh_check_date_between(date.fromisoformat(value), start_str, end_str)
    return "" if result is None else result
