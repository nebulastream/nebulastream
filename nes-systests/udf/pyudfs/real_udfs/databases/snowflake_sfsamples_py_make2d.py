# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["01d10700008fc2f5285ccf2a40c3f5285c8f424a400000000000803140"], ["01d20700000300000000000000000000000000000000000000000000000000f03f000000000000f03f000000000000f03f00000000000000400000000000000040000000000000e03f0000000000000840"], ["01b90b0000b3ea73b5157f52c0c7293a92cb5f44400000000000b07b4000000040fc54d941"], ["0101000000a835cd3b4ed1024076e09c11a56d4840"], ["01d20700000200000050fc1873d79a5ec0d0d556ec2fe3424000000000000000004182e2c7988f5dc0f46c567dae0641400000000000788140"]]
# real-udf: origin=databases id=snowflake_sfsamples_py_make2d requires=gdal,shapely wrapped=1 ported=0
# Wrapper: The UDF returns a GeoJSON dict (Snowflake GEOGRAPHY); case_entry returns it as json.dumps(..., sort_keys=True). The input is already hex WKB text.
# Source: https://github.com/Snowflake-Labs/sf-samples/blob/96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f/samples/geospatial/Python%20UDFs/PY_MAKE2D.sql#L3
# Commit: 96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f
# License: Apache-2.0
# Python body of the CREATE FUNCTION ... LANGUAGE PYTHON statement (handler-based, copied as is).

from osgeo import ogr
import shapely.wkb
from shapely.geometry import mapping
def udf(g1):
    geom = bytes.fromhex(g1)
    ogr_geom = ogr.CreateGeometryFromWkb(geom)
    ogr_geom.SetMeasured(False)
    shapely_geom = shapely.wkb.loads(ogr_geom.ExportToIsoWkb().hex(), hex = True)
    return mapping(shapely_geom)


def case_entry(g1):
    import json
    return json.dumps(udf(g1), sort_keys=True)
