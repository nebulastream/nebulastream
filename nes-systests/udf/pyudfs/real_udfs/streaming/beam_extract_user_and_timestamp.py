# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"title\": \"Apache Beam\", \"timestamp\": 1446145800, \"contributor_username\": \"Jwestbrook\", \"contributor_id\": 4171}"], ["{\"title\": \"Stream processing\", \"timestamp\": 1446146122, \"contributor_username\": \"Gregbard\", \"comment\": \"fix link\"}"], ["{\"title\": \"Dataflow\", \"timestamp\": 1446149001, \"contributor_ip\": \"198.51.100.7\"}"], ["{\"title\": \"MapReduce\", \"timestamp\": 1446152460.5, \"contributor_username\": \"Ömer K.\"}"], ["{\"title\": \"Lambda architecture\", \"timestamp\": 1446160000, \"contributor_username\": \"Pinkbeast\", \"minor\": true}"]]
# real-udf: origin=streaming id=beam_extract_user_and_timestamp requires=apache-beam wrapped=1 ported=0
# Wrapper: case_entry returns the TimestampedValue as "user|timestamp", or "" when the edit has no contributor user name (the UDF returns None).
# Imports: the original module's imports that this code uses.

import json

from apache_beam.transforms.window import TimestampedValue

# Source: https://github.com/apache/beam/blob/7f2fd8b6b080ad65ee49a2c75045fc34bec7685c/sdks/python/apache_beam/examples/complete/top_wikipedia_sessions.py#L64-L70
# Repository: https://github.com/apache/beam (commit 7f2fd8b6b080ad65ee49a2c75045fc34bec7685c)
# License: Apache-2.0
# System: Apache Beam
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def extract_user_and_timestamp(element):
  """Extracts user and timestamp representing a Wikipedia edit."""
  table_row = json.loads(element)
  if 'contributor_username' in table_row:
    user_name = table_row['contributor_username']
    timestamp = table_row['timestamp']
    return TimestampedValue(user_name, timestamp)


def case_entry(element):
    value = extract_user_and_timestamp(element)
    return "" if value is None else "|".join((value.value, str(value.timestamp)))
