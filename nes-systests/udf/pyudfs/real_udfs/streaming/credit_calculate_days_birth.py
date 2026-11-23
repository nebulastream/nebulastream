# codon-fuzz: entry=calculate_days_birth args=VARSIZED,VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1985-03-14", "2024-06-01T10:15:30Z"], ["-5000", "2023-11-20 08:00:00"], ["9131", "2024-01-15T12:00:00+00:00"], ["1990-12-31", "2024-02-29"], ["1972-07-04", "2025-01-02T23:59:59.123456Z"], ["2001-09-30T00:00:00", "2024-09-30 17:45:12.512000"]]
# real-udf: origin=streaming id=credit_calculate_days_birth requires=python-dateutil wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.
# Imports: the original module's imports.

from datetime import date, timedelta
from typing import Optional

# Source: https://github.com/ngnquanq/credit-risk-ml-system/blob/ff2442ed39d3f8b377e4aa52e668a494a5bd4b4b/application/flink/jobs/cdc_udfs.py#L61-L108
# Repository: https://github.com/ngnquanq/credit-risk-ml-system (commit ff2442ed39d3f8b377e4aa52e668a494a5bd4b4b)
# License: MIT
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def _date_from_days_since_epoch(days: int) -> Optional[date]:
    """Helper (non-UDF) to convert days since epoch to date.
    Kept as a plain function so other UDFs can call it without embedding
    a UDF object (which is not picklable by cloudpickle).
    """
    if days is None:
        return None
    try:
        return date(1970, 1, 1) + timedelta(days=int(days))
    except Exception:
        return None


def calculate_days_birth(birth_date_str: str, created_at_str: str) -> Optional[int]:
    """
    Calculate days_birth field (negative days from birth to application).
    Handles both ISO date strings and integer days since epoch.

    Args:
        birth_date_str: Birth date as string (ISO format or days since epoch)
        created_at_str: Application creation timestamp

    Returns:
        Days between birth and application (typically negative)
    """
    if not birth_date_str or not created_at_str:
        return None

    try:
        from dateutil import parser as dtparser

        # Parse created_at
        created_at = dtparser.parse(created_at_str).date()

        # Handle birth_date - could be ISO string or integer days
        if birth_date_str.isdigit() or (birth_date_str.startswith('-') and birth_date_str[1:].isdigit()):
            # Integer days since epoch
            birth_date = _date_from_days_since_epoch(int(birth_date_str))
        else:
            # ISO string format
            birth_date = dtparser.parse(birth_date_str).date()

        if birth_date and created_at:
            return (birth_date - created_at).days
        return None
    except Exception:
        return None
