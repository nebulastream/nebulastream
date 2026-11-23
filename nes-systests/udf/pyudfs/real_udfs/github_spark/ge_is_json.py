# codon-fuzz: entry=is_json args=VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"user_id\": 42, \"event\": \"click\"}"], ["[1, 2, 3]"], ["not json"], ["{\"user\": \"x\", "], ["42"], ["\"quoted\""], ["NaN"], ["{'single': 'quotes'}"]]
# real-udf: origin=github-spark id=ge_is_json requires= wrapped=0 ported=0
# Source: https://github.com/fivetran/great_expectations/blob/e08d1a800c8b759c84b820e420f5699392c53a98/great_expectations/expectations/metrics/column_map_metrics/column_values_json_parseable.py#L33-L38
# Repository: fivetran/great_expectations @ commit e08d1a800c8b759c84b820e420f5699392c53a98
# License: Apache-2.0
# Copied verbatim from great_expectations/expectations/metrics/column_map_metrics/column_values_json_parseable.py (L33-L38); only the module's own import lines that the code uses are prepended.

import json


def is_json(val):
    try:
        json.loads(val)
        return True
    except Exception:
        return False
