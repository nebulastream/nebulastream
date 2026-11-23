# codon-fuzz: entry=area_handler args=FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[1.0], [2.5], [0.75], [10.0], [3.2], [120.5]]
# real-udf: origin=databases id=bigquery_docs_area_handler requires=scipy wrapped=0 ported=0
# Source: https://docs.cloud.google.com/bigquery/docs/user-defined-functions-python
# Commit: n/a (documentation page, fetched 2026-11-23)
# License: Apache-2.0
# Google Cloud documentation; per the Google Developers Site Policy, code samples are licensed under Apache-2.0.

import scipy

def area_handler(radius):
  return scipy.constants.pi*radius*radius
