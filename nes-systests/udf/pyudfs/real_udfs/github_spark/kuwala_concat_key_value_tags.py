# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[{\"key\": \"amenity\", \"value\": \"cafe\"}, {\"key\": \"name\", \"value\": \"Blue Bottle\"}]"], ["[{\"key\": \"shop\", \"value\": \"supermarket\"}, {\"key\": \"opening_hours\", \"value\": \"Mo-Sa 08:00-20:00\"}, {\"key\": \"wheelchair\", \"value\": \"yes\"}]"], ["[]"], ["[{\"key\": \"building\", \"value\": \"yes\"}]"], ["[{\"key\": \"addr:street\", \"value\": \"Unter den Linden\"}, {\"key\": \"addr:housenumber\", \"value\": \"77\"}]"]]
# real-udf: origin=github-spark id=kuwala_concat_key_value_tags requires= wrapped=1 ported=0
# Wrapper: the UDF takes an array of {key, value} structs and returns an array of strings; case_entry
# Wrapper: reads the structs from a JSON array and returns the result as JSON text.
# Source: https://github.com/kuwala-io/kuwala/blob/44d73c2cd0be03acc5984b79597f40695fdeff4c/kuwala/common/python_utils/src/spark_udfs.py#L104-L106
# Repository: kuwala-io/kuwala @ commit 44d73c2cd0be03acc5984b79597f40695fdeff4c
# License: Apache-2.0
# Copied verbatim from kuwala/common/python_utils/src/spark_udfs.py (L104-L106); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=ArrayType(elementType=StringType()))

def concat_list_of_key_value_pairs(tags):
    return list(map(lambda t: f'{t["key"]}={t["value"]}', tags))


import json


def case_entry(tags_json):
    return json.dumps(concat_list_of_key_value_pairs(json.loads(tags_json)))
