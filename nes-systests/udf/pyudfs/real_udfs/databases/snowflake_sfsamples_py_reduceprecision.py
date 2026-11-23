# codon-fuzz: entry=case_entry args=VARSIZED,INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"type\": \"Point\", \"coordinates\": [13.404954, 52.520008]}", 2], ["{\"type\": \"LineString\", \"coordinates\": [[-73.985664, 40.748441], [-73.968285, 40.785091]]}", 3], ["{\"type\": \"Polygon\", \"coordinates\": [[[2.294481, 48.85837], [2.295, 48.8584], [2.2948, 48.859], [2.294481, 48.85837]]]}", 4], ["{\"type\": \"Point\", \"coordinates\": [151.215297, -33.856784]}", 0], ["{\"type\": \"MultiPoint\", \"coordinates\": [[0.123456789, 1.987654321], [10.5, 20.25]]}", 5]]
# real-udf: origin=databases id=snowflake_sfsamples_py_reduceprecision requires=shapely wrapped=1 ported=0
# Wrapper: Snowflake passes a GEOGRAPHY as a GeoJSON dict and takes a dict back; case_entry parses GeoJSON text and returns the result as json.dumps(..., sort_keys=True).
# Source: https://github.com/Snowflake-Labs/sf-samples/blob/96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f/samples/geospatial/Python%20UDFs/PY_REDUCEPRECISION.sql#L3
# Commit: 96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f
# License: Apache-2.0
# Python body of the CREATE FUNCTION ... LANGUAGE PYTHON statement (handler-based, copied as is).

from shapely.geometry import shape, mapping
from shapely import wkt
def udf(geo, n):
    if n < 0:
        raise ValueError('Number of digits must be positive')
    else:
        g1 = shape(geo)
        updated_shape = wkt.loads(wkt.dumps(g1, rounding_precision=n))
        return mapping(updated_shape)


def case_entry(geo, n):
    import json
    return json.dumps(udf(json.loads(geo), n), sort_keys=True)
