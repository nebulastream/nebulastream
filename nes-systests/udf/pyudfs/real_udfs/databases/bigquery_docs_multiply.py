# codon-fuzz: entry=multiply args=FLOAT64,FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[19.99, 3.0], [2.5, 4.0], [-12.75, 0.2], [1000000.0, 1.07], [0.0, 42.5], [3.3333, 3.0]]
# real-udf: origin=databases id=bigquery_docs_multiply requires= wrapped=0 ported=0
# Source: https://docs.cloud.google.com/bigquery/docs/user-defined-functions-python
# Commit: n/a (documentation page, fetched 2026-11-23)
# License: Apache-2.0
# Google Cloud documentation; per the Google Developers Site Policy, code samples are licensed under Apache-2.0.

def multiply(x, y):
    return x * y
