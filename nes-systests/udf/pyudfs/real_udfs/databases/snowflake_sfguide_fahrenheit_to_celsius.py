# codon-fuzz: entry=main args=FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[32.0], [212.0], [98.6], [-40.0], [0.0], [71.3]]
# real-udf: origin=databases id=snowflake_sfguide_fahrenheit_to_celsius requires= wrapped=0 ported=0
# Source: https://github.com/Snowflake-Labs/sfguide-data-engineering-with-snowpark-python/blob/44c92001766275725eaa8c29d94e81562f00c03e/steps/05_fahrenheit_to_celsius_udf/fahrenheit_to_celsius_udf/function.py#L14
# Commit: 44c92001766275725eaa8c29d94e81562f00c03e
# License: Apache-2.0
# Handler function only (local-debug __main__ block omitted).

def main(temp_f: float) -> float:
    return (float(temp_f) - 32) * (5/9)
