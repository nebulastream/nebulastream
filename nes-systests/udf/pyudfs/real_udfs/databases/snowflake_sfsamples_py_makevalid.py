# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"type\": \"Polygon\", \"coordinates\": [[[0, 0], [2, 2], [2, 0], [0, 2], [0, 0]]]}"], ["{\"type\": \"Polygon\", \"coordinates\": [[[13.37, 52.51], [13.38, 52.51], [13.38, 52.52], [13.37, 52.52], [13.37, 52.51]]]}"], ["{\"type\": \"Polygon\", \"coordinates\": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]], [[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]]]}"], ["{\"type\": \"LineString\", \"coordinates\": [[0, 0], [1, 1], [2, 0]]}"], ["{\"type\": \"Point\", \"coordinates\": [8.54, 47.37]}"]]
# real-udf: origin=databases id=snowflake_sfsamples_py_makevalid requires=shapely wrapped=1 ported=0
# Wrapper: Snowflake passes a GEOGRAPHY as a GeoJSON dict and takes a dict back; case_entry parses GeoJSON text and returns the result as json.dumps(..., sort_keys=True).
# Source: https://github.com/Snowflake-Labs/sf-samples/blob/96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f/samples/geospatial/Python%20UDFs/PY_MAKEVALID.sql#L1
# Commit: 96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f
# License: Apache-2.0
# Python body of the CREATE FUNCTION ... LANGUAGE PYTHON statement (handler-based, copied as is).

import shapely
from shapely.geometry import shape, mapping
from shapely.validation import make_valid
def udf(geo):
    g1 = shape(geo)
    if g1.is_valid == True:
        g1 = g1.buffer(0.000001, resolution = 1, join_style = 1)
        g1 = g1.simplify(0.000001)
        fixed_shape = g1
    else:
        fixed_shape = make_valid(g1)
    return  mapping(fixed_shape)


def case_entry(geo):
    import json
    return json.dumps(udf(json.loads(geo)), sort_keys=True)
