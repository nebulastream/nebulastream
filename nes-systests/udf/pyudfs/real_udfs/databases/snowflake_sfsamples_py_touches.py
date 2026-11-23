# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"type\": \"Polygon\", \"coordinates\": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}", "{\"type\": \"Polygon\", \"coordinates\": [[[1, 0], [2, 0], [2, 1], [1, 1], [1, 0]]]}"], ["{\"type\": \"Polygon\", \"coordinates\": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}", "{\"type\": \"Polygon\", \"coordinates\": [[[0.5, 0.5], [2, 0.5], [2, 2], [0.5, 2], [0.5, 0.5]]]}"], ["{\"type\": \"LineString\", \"coordinates\": [[0, 0], [1, 1]]}", "{\"type\": \"Point\", \"coordinates\": [1, 1]}"], ["{\"type\": \"Point\", \"coordinates\": [13.405, 52.52]}", "{\"type\": \"Point\", \"coordinates\": [2.3522, 48.8566]}"], ["{\"type\": \"Polygon\", \"coordinates\": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}", "{\"type\": \"Point\", \"coordinates\": [0.5, 1]}"]]
# real-udf: origin=databases id=snowflake_sfsamples_py_touches requires=shapely wrapped=1 ported=0
# Wrapper: Snowflake passes a GEOGRAPHY as a GeoJSON dict; case_entry parses GeoJSON text.
# Source: https://github.com/Snowflake-Labs/sf-samples/blob/96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f/samples/geospatial/Python%20UDFs/PY_TOUCHES.sql#L3
# Commit: 96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f
# License: Apache-2.0
# Python body of the CREATE FUNCTION ... LANGUAGE PYTHON statement (handler-based, copied as is).

from shapely.geometry import shape, mapping
def udf(g1, g2):
    geom1 = shape(g1)
    geom2 = shape(g2)
    return geom1.touches(geom2)


def case_entry(g1, g2):
    import json
    return udf(json.loads(g1), json.loads(g2))
