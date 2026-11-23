# codon-fuzz: entry=case_entry args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"type\": \"MultiPoint\", \"coordinates\": [[13.405, 52.52], [2.3522, 48.8566], [-0.1276, 51.5072]]}"], ["{\"type\": \"MultiLineString\", \"coordinates\": [[[0, 0], [1, 1]], [[2, 2], [3, 5]]]}"], ["{\"type\": \"MultiPolygon\", \"coordinates\": [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]]}"], ["{\"type\": \"GeometryCollection\", \"geometries\": [{\"type\": \"Point\", \"coordinates\": [1, 2]}, {\"type\": \"Point\", \"coordinates\": [3, 4]}, {\"type\": \"LineString\", \"coordinates\": [[0, 0], [5, 5]]}]}"], ["{\"type\": \"MultiPoint\", \"coordinates\": [[0, 0], [0, 1], [1, 0], [1, 1], [2, 2]]}"]]
# real-udf: origin=databases id=snowflake_sfsamples_py_numgeographys requires=shapely wrapped=1 ported=0
# Wrapper: Snowflake passes a GEOGRAPHY as a GeoJSON dict; case_entry parses GeoJSON text.
# Source: https://github.com/Snowflake-Labs/sf-samples/blob/96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f/samples/geospatial/Python%20UDFs/PY_NUMGEOMETRIES.sql#L5
# Commit: 96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f
# License: Apache-2.0
# Python body of the CREATE FUNCTION ... LANGUAGE PYTHON statement (handler-based, copied as is).

from shapely.geometry import shape, mapping
def udf(geo):
    if geo['type'] not in ('MultiPoint', 'MultiLineString', 'MultiPolygon', 'GeometryCollection'):
        raise ValueError('Must be a composite geometry type')
    else:
        g1 = shape(geo)
        return len(g1.geoms)


def case_entry(geo):
    import json
    return udf(json.loads(geo))
