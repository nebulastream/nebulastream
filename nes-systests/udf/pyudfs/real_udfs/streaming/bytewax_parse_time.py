# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1594512094.3859746"], ["1594512094.7355459"], ["1594512098.4366123"], ["1595321940.0"], ["1594512106.869076"], ["1594512104"]]
# real-udf: origin=streaming id=bytewax_parse_time requires= wrapped=1 ported=0
# Wrapper: case_entry builds the CSV row dict with the "ts" field and returns the parsed timestamp in ISO format.
# Imports: the original module's imports that this code uses.

from datetime import datetime, timezone

# Source: https://github.com/bytewax/bytewax/blob/2fe2cd3b220195aa614c3effb50f31925dcaa836/docs/tutorials/profiling-time-series-data/timeseries_dataflow.py#L35-L38
# Repository: https://github.com/bytewax/bytewax (commit 2fe2cd3b220195aa614c3effb50f31925dcaa836)
# License: Apache-2.0
# System: Bytewax
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def parse_time(reading_data):
    """Parse the timestamp in the reading data."""
    reading_data["ts"] = datetime.fromtimestamp(float(reading_data["ts"]), timezone.utc)
    return reading_data


def case_entry(ts):
    return parse_time({"ts": ts})["ts"].isoformat()
