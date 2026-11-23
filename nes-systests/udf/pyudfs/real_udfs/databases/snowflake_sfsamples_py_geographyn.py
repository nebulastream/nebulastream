# codon-fuzz: entry=case_entry args=VARSIZED,INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"type\": \"MultiPoint\", \"coordinates\": [[13.405, 52.52], [2.3522, 48.8566], [-0.1276, 51.5072]]}", 1], ["{\"type\": \"MultiLineString\", \"coordinates\": [[[0, 0], [1, 1]], [[2, 2], [3, 5], [4, 4]]]}", 0], ["{\"type\": \"MultiPolygon\", \"coordinates\": [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]], [[[5, 5], [6, 5], [6, 6], [5, 5]]]]}", 1], ["{\"type\": \"GeometryCollection\", \"geometries\": [{\"type\": \"Point\", \"coordinates\": [8.54, 47.37]}, {\"type\": \"LineString\", \"coordinates\": [[8.5, 47.3], [8.6, 47.4]]}]}", 1], ["{\"type\": \"MultiPoint\", \"coordinates\": [[-73.9857, 40.7484]]}", 0]]
# real-udf: origin=databases id=snowflake_sfsamples_py_geographyn requires=shapely wrapped=1 ported=0
# Wrapper: Snowflake passes a GEOGRAPHY as a GeoJSON dict and takes a dict back; case_entry parses GeoJSON text and returns the result as json.dumps(..., sort_keys=True).
# Source: https://github.com/Snowflake-Labs/sf-samples/blob/96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f/samples/geospatial/Python%20UDFs/PY_GEOMETRYN.sql#L4
# Commit: 96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f
# License: Apache-2.0
# Python body of the CREATE FUNCTION ... LANGUAGE PYTHON statement (handler-based, copied as is).

from shapely.geometry import shape, mapping
def udf(geo, n):
    if geo['type'] not in ('MultiPoint', 'MultiLineString', 'MultiPolygon', 'GeometryCollection'):
        raise ValueError('Must be a composite geometry type')
    if n < 0:
        raise ValueError('N must be positive')
    g1 = shape(geo)
    if n > len(g1.geoms) - 1:
        raise ValueError('N out of range')
    else:
        return mapping(g1.geoms[n])


def case_entry(geo, n):
    import json
    return json.dumps(udf(json.loads(geo), n), sort_keys=True)
