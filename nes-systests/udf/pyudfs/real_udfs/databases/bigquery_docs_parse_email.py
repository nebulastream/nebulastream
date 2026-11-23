# codon-fuzz: entry=parse_email args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"id\": 1042, \"email\": \"jane.doe@example.com\", \"plan\": \"pro\"}"], ["{\"email\": \"m.mueller@firma.de\", \"signup\": \"2024-05-01T10:12:00Z\"}"], ["{\"user\": {\"name\": \"Ana\"}, \"email\": \"ana+news@gmail.com\"}"], ["{\"email\": \"ops@acme.io\", \"tags\": [\"admin\", \"oncall\"], \"active\": true}"], ["{\"email\": \"\", \"id\": 7}"], ["{\"email\": \"li.wei@uni-berlin.de\", \"score\": 98.5}"]]
# real-udf: origin=databases id=bigquery_docs_parse_email requires= wrapped=0 ported=0
# Source: https://docs.cloud.google.com/bigquery/docs/user-defined-functions-python
# Commit: n/a (documentation page, fetched 2026-11-23)
# License: Apache-2.0
# Google Cloud documentation; per the Google Developers Site Policy, code samples are licensed under Apache-2.0.

import json
import logging
def parse_email(payload):
  try:
    # Attempt to parse the JSON and extract the email
    data = json.loads(payload)
    return data.get('email')
  except Exception as e:
    # Log the exact bad data and the error to Cloud Logging
    logging.info(f"Failed to parse payload: '{payload}'. Error: {e}")
    # Return None (NULL in BigQuery) to prevent the query from failing.
    return None
