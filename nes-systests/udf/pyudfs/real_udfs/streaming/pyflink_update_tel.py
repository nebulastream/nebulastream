# codon-fuzz: entry=update_tel args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"name\": \"flink\", \"tel\": 123, \"addr\": {\"country\": \"Germany\", \"city\": \"Berlin\"}}"], ["{\"name\": \"hello\", \"tel\": 135, \"addr\": {\"country\": \"China\", \"city\": \"Shanghai\"}}"], ["{\"name\": \"apache flink\", \"tel\": 32, \"addr\": {\"country\": \"China\", \"city\": \"Beijing\"}}"], ["{\"name\": \"Zoë\", \"tel\": 4915112345678, \"addr\": {\"country\": \"Österreich\", \"city\": \"Wien\"}}"], ["{\"tel\": -1, \"tags\": [\"vip\", \"beta\"], \"active\": true, \"score\": 9.5}"]]
# real-udf: origin=streaming id=pyflink_update_tel requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.
# Imports: the original module's imports.

import json

# Source: https://github.com/apache/flink/blob/b1f051d09c4111898902167428199c00747a5076/flink-python/pyflink/examples/table/process_json_data_with_udf.py#L52-L56
# Repository: https://github.com/apache/flink (commit b1f051d09c4111898902167428199c00747a5076)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def update_tel(data):
    json_data = json.loads(data)
    json_data['tel'] += 1
    return json.dumps(json_data)
