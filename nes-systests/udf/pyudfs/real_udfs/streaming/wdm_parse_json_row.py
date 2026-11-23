# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"car\": \"Tesla Model 3\", \"attr\": \"red\"}"], ["{\"car\": \"VW Golf\", \"attr\": \"diesel\", \"year\": 2019}"], ["{\"car\": \"Fiat 500\"}"], ["{\"attr\": 42}"], ["{\"car\": \"BMW i3\", \"attr\": \"electric\""], ["{\"car\": \"Škoda Octavia\", \"attr\": \"combi\"}"]]
# real-udf: origin=streaming id=wdm_parse_json_row requires=apache-flink wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.
# Wrapper: case_entry returns the Row fields joined with "|".
# Imports: the original module's imports that this code uses.

import json

from pyflink.table import DataTypes, Row

# Source: https://github.com/wdm0006/flink-python-examples/blob/b0a55a8efe82476244bc3b410729ba33be65cf06/data_enrichment/data_enrichment.py#L50-L61
# Repository: https://github.com/wdm0006/flink-python-examples (commit b0a55a8efe82476244bc3b410729ba33be65cf06)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def parse_json(json_str):
    try:
        data = json.loads(json_str)
        # Handle potential missing keys gracefully
        return Row(str(data.get('car', '')), str(data.get('attr', '')))
    except json.JSONDecodeError:
        # Handle lines that are not valid JSON
        return Row(None, None)


def case_entry(json_str):
    return "|".join(str(value) for value in parse_json(json_str))
