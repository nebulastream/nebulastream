# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Mon Jan 07 12:30:00 -0800 2013"], ["Tue Aug 15 08:05:59 -0700 2017"], ["Sat Feb 29 23:59:59 +0000 2020"], ["Wed Dec 31 00:00:01 -0800 2008"], ["Fri Oct 03 17:45:12 +0100 2014"], ["Sun Jun 01 06:00:00 -0700 2014"]]
# real-udf: origin=github-spark id=goodreads_string_to_datetime requires= wrapped=1 ported=0
# Wrapper: the UDF returns a datetime; case_entry returns its ISO 8601 text.
# Source: https://github.com/san089/goodreads_etl_pipeline/blob/4cfabe002717804a1b029a6e22d3ac09409e3434/src/goodreads_udf.py#L10-L15
# Repository: san089/goodreads_etl_pipeline @ commit 4cfabe002717804a1b029a6e22d3ac09409e3434
# License: MIT
# Copied verbatim from src/goodreads_udf.py (L10-L15); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(TimestampType())

from time import strptime
from datetime import datetime


def stringtodatetime(datestring):
    x = datestring.split()
    day, month, year = int(x[2]), strptime(x[1],'%b').tm_mon, int(x[5])
    hour, minute, second = [int(val) for val in x[3].split(":")]
    return datetime(year = year, month = month, day = day, hour = hour, minute = minute, second = second)


def case_entry(datestring):
    return stringtodatetime(datestring).isoformat()
