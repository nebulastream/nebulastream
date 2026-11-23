# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[[13.404954, 52.520008], [13.405, 52.521], [13.407, 52.5205], [13.404954, 52.520008]]"], ["[[13.38, 52.51], [13.39, 52.515], [13.4, 52.512]]"], ["[[-73.9857, 40.7484], [-73.9851, 40.7489], [-73.9845, 40.7482], [-73.985, 40.7478], [-73.9857, 40.7484]]"], ["[[2.2945, 48.8584], [2.3522, 48.8566]]"], ["[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]"]]
# real-udf: origin=github-spark id=kuwala_geojson_from_coordinates requires=shapely wrapped=1 ported=0
# Wrapper: the UDF takes an array of [lng, lat] pairs; case_entry reads it from a JSON array.
# Source: https://github.com/kuwala-io/kuwala/blob/44d73c2cd0be03acc5984b79597f40695fdeff4c/kuwala/common/python_utils/src/spark_udfs.py#L50-L69
# Repository: kuwala-io/kuwala @ commit 44d73c2cd0be03acc5984b79597f40695fdeff4c
# License: Apache-2.0
# Copied verbatim from kuwala/common/python_utils/src/spark_udfs.py (L50-L69); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

import json
from shapely.geometry import shape


def create_geo_json_based_on_coordinates(coordinates: [[float]]):
    if not coordinates or len(coordinates) < 1:
        return

    last_index = len(coordinates) - 1
    geo_json_type = "Polygon"

    if (coordinates[0][0] != coordinates[last_index][0]) or (
        coordinates[0][1] != coordinates[last_index][1]
    ):
        geo_json_type = "LineString"

    geo_json_coordinates = [coordinates] if geo_json_type == "Polygon" else coordinates
    geo_json = json.dumps(dict(type=geo_json_type, coordinates=geo_json_coordinates))

    if shape(json.loads(geo_json)).is_valid:
        return geo_json

    return None


def case_entry(coordinates_json):
    return create_geo_json_based_on_coordinates(json.loads(coordinates_json))
