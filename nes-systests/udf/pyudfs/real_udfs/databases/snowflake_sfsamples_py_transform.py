# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"type\": \"Point\", \"coordinates\": [13.405, 52.52]}", "EPSG:4326", "EPSG:3857"], ["{\"type\": \"LineString\", \"coordinates\": [[-73.9857, 40.7484], [-73.9683, 40.7851]]}", "EPSG:4326", "EPSG:32618"], ["{\"type\": \"Point\", \"coordinates\": [1489199.0, 6894018.0]}", "EPSG:3857", "EPSG:4326"], ["{\"type\": \"Polygon\", \"coordinates\": [[[8.5, 47.3], [8.6, 47.3], [8.6, 47.4], [8.5, 47.3]]]}", "EPSG:4326", "EPSG:2056"], ["{\"type\": \"Point\", \"coordinates\": [2.3522, 48.8566]}", "EPSG:4326", "EPSG:2154"]]
# real-udf: origin=databases id=snowflake_sfsamples_py_transform requires=pyproj,shapely wrapped=1 ported=0
# Wrapper: Snowflake passes a GEOGRAPHY as a GeoJSON dict and takes a dict back; case_entry parses GeoJSON text and returns the result as json.dumps(..., sort_keys=True).
# Source: https://github.com/Snowflake-Labs/sf-samples/blob/96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f/samples/geospatial/Python%20UDFs/PY_TRANSFORM.sql#L1
# Commit: 96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f
# License: Apache-2.0
# Python body of the CREATE FUNCTION ... LANGUAGE PYTHON statement (handler-based, copied as is).

import pyproj
import shapely
from shapely.ops import transform
from shapely.geometry import shape
from shapely import wkb, wkt
def udf(g1, srid_from, srid_to):
    source_srid = pyproj.CRS(srid_from)
    target_srid = pyproj.CRS(srid_to)
    project = pyproj.Transformer.from_crs(source_srid, target_srid, always_xy=True).transform
    shape_wgs = shape(g1)
    shape_tr = transform(project, shape_wgs)
    return shapely.geometry.mapping(shape_tr)


def case_entry(g1, srid_from, srid_to):
    import json
    return json.dumps(udf(json.loads(g1), srid_from, srid_to), sort_keys=True)
