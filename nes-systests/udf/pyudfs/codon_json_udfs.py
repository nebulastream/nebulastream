# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""UDFs written against Python's json API, run by Codon with the simdjson-backed `json` plugin module
(nes-codon-json-plugin). The expected results in CodonJson.test are what CPython returns for the same functions.

Results that combine several checks are joined with "|", because the file sink does not quote commas.
"""

import json


def roundtrip(doc):
    """Parse a JSON document and serialize it again (CPython's default format)."""
    return json.dumps(json.loads(doc))


def count(doc):
    """Number of elements for an array, number of keys for an object."""
    return len(json.loads(doc))


def nested_len(doc):
    """Indexing by key and position, then len."""
    return len(json.loads(doc)["items"][1])


def lowered_names(doc):
    """str methods on parsed strings; the results flow into Codon's sorted and str.join."""
    return "|".join(sorted([name.lower() for name in json.loads(doc)["names"]]))


def sorted_tokens(doc):
    """split on a parsed string, as the StreamUDFBench sortname helper does."""
    return " ".join(sorted(json.loads(doc)["names"][0].split(" ")))


def dict_get(doc):
    """dict.get returns None for a missing key and for a JSON null."""
    data = json.loads(doc)
    checks = [data.get("missing") is None, data.get("b") is None, data.get("a") is None, not data.get("missing")]
    return "|".join([str(check) for check in checks]) + "|" + data.get("a").lower()


def comparisons(doc):
    """Equality with Python's True == 1 == 1.0 rules, and the in operator on keys."""
    data = json.loads(doc)
    checks = [data["ok"] == 1, data["ok"] == True, data["n"] == 2.5, data["items"][0][0] == 1, "n" in data, "x" in data]
    return "|".join([str(check) for check in checks])


def distinct_count(doc):
    """Parsed values are hashable, so they can go into a set."""
    return len(set([value for value in json.loads(doc)["dupes"]]))


def sorted_dump(doc):
    """sorted over parsed values, then json.dumps of the resulting list."""
    return json.dumps(sorted(json.loads(doc)["dupes"])[:1])


def conversions(doc):
    """str, int, float and bool of parsed values, and the Python repr of a nested value."""
    data = json.loads(doc)
    parts = [str(data["n"]), str(int(data["items"][0][1])), str(float(data["n"])), str(bool(data["b"])), str(data["flag"])]
    return "|".join(parts)
