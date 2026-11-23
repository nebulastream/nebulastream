# codon-fuzz: entry=drop_abstract_inverted_index args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"id\": \"https://openalex.org/W2741809807\", \"title\": \"The state of OA\", \"abstract_inverted_index\": {\"Despite\": [0], \"growing\": [1]}, \"publication_year\": 2018}"], ["{\"id\": \"https://openalex.org/W1234\", \"title\": \"Saúde pública no Brasil\", \"language\": \"pt\"}"], ["{\"id\": \"https://openalex.org/W5678\", \"abstract_inverted_index\": null, \"cited_by_count\": 12}"], ["not a json line"], ["[1, 2, 3]"], ["{\"id\": \"https://openalex.org/W42\", \"authorships\": [{\"author\": {\"display_name\": \"José Silva\"}}], \"abstract_inverted_index\": {\"Um\": [0]}}"]]
# real-udf: origin=github-spark id=scielo_drop_abstract_index requires= wrapped=0 ported=0
# Note: the [1, 2, 3] row is a JSON array: list.pop() raises TypeError inside the try, so the input is returned.
# Source: https://github.com/scieloorg/IndexLoader/blob/bf6bea09f308aed626838cafec814e23aae0fc63/jobs/split.py#L30-L40
# Repository: scieloorg/IndexLoader @ commit bf6bea09f308aed626838cafec814e23aae0fc63
# License: BSD-3-Clause
# Copied verbatim from jobs/split.py (L30-L40); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(returnType=T.StringType())

import json


def drop_abstract_inverted_index(raw: str) -> str:
    if raw is None:
        return None
    try:
        obj = json.loads(raw)
        obj.pop("abstract_inverted_index", None)
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        # Se tiver alguma linha ruim, mantém como está (ou você pode retornar None para descartar)
        return raw
