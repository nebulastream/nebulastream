# codon-fuzz: entry=square_area args=FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[3.0], [12.5], [0.1], [1500.0], [7.25], [0.0]]
# real-udf: origin=databases id=bigquery_docs_square_area requires= wrapped=0 ported=0
# Source: https://docs.cloud.google.com/bigquery/docs/user-defined-functions-python
# Commit: n/a (documentation page, fetched 2026-11-23)
# License: Apache-2.0
# Google Cloud documentation; per the Google Developers Site Policy, code samples are licensed under Apache-2.0.

def square_area(length):
  return length*length
