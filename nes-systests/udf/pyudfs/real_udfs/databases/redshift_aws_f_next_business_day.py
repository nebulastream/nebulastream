# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["2024-07-03"], ["2024-07-05"], ["2024-12-24"], ["2024-11-27"], ["2023-12-29"], ["2024-01-12"], ["2024-03-13"]]
# real-udf: origin=databases id=redshift_aws_f_next_business_day requires=pandas wrapped=1 ported=0
# Wrapper: Redshift passes a DATE as datetime.date; case_entry parses ISO text and formats the resulting pandas Timestamp as YYYY-MM-DD.
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_next_business_day%28date%29/function.sql#L9
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_next_business_day(dt):
    import pandas
    from pandas.tseries.offsets import CustomBusinessDay
    from pandas.tseries.holiday import USFederalHolidayCalendar

    bday_us = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    return dt + bday_us


def case_entry(dt):
    from datetime import date
    return f_next_business_day(date.fromisoformat(dt)).strftime("%Y-%m-%d")
