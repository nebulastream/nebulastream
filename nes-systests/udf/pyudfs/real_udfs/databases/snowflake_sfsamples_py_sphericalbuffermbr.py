# codon-fuzz: entry=case_entry args=FLOAT64,FLOAT64,FLOAT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[13.405, 52.52, 1000.0], [-73.9857, 40.7484, 500.0], [151.2153, -33.8568, 2500.0], [0.0, 0.0, 10000.0], [8.5417, 47.3769, 150.0]]
# real-udf: origin=databases id=snowflake_sfsamples_py_sphericalbuffermbr requires=pyproj,shapely wrapped=1 ported=0
# Wrapper: The UDF returns a GeoJSON dict (Snowflake GEOGRAPHY); case_entry returns it as json.dumps(..., sort_keys=True).
# Source: https://github.com/Snowflake-Labs/sf-samples/blob/96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f/samples/geospatial/Python%20UDFs/PY_SPHERICALBUFFERMBR.sql#L4
# Commit: 96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f
# License: Apache-2.0
# Python body of the CREATE FUNCTION ... LANGUAGE PYTHON statement (handler-based, copied as is).

from shapely.geometry import box, mapping
import pyproj
def udf(longitude, latitude, radius_meters):
    geod = pyproj.Geod(ellps="WGS84")
    n_corner_long, n_corner_lat, _ = geod.fwd(longitude, latitude, 0, radius_meters)
    s_corner_long, s_corner_lat, _ = geod.fwd(longitude, latitude, 180, radius_meters)
    w_corner_long, w_corner_lat, _ = geod.fwd(longitude, latitude, -90, radius_meters)
    e_corner_long, e_corner_lat, _ = geod.fwd(longitude, latitude, 90, radius_meters)    
    mbr = box(w_corner_long, n_corner_lat, e_corner_long, s_corner_lat)
    return mapping(mbr)


def case_entry(longitude, latitude, radius_meters):
    import json
    return json.dumps(udf(longitude, latitude, radius_meters), sort_keys=True)
