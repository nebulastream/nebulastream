# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["{\"id\": 7, \"price\": 1.10, \"name\": \"caf\\u00e9\", \"ok\": true}"], ["{\"id\": -3, \"price\": 2, \"name\": \"\", \"ok\": false}"], ["{\"id\": 1.0, \"price\": 0.1, \"name\": \"x\", \"ok\": null}"]]
import json


def udf(s):
    try:
        d = json.loads(s)
        pid = int(d["id"])
        price = float(d["price"])
        name = str(d["name"])
        ok = bool(d["ok"])
    except Exception:
        return "bad"
    return f"{pid}|{price}|{name.upper()}|{ok}|{price * 3}"
