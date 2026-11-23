# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED,VARSIZED,VARSIZED,BOOLEAN returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"request\": {\"verb\": \"GET\", \"uri\": \"https://api.acme.io/v1/orders?limit=20\", \"headers\": {\"User-Agent\": \"okhttp/4.12.0\"}}, \"response\": {\"status\": 200, \"time\": 124}}", "response.status", "equals", "\"200\"", false], ["{\"request\": {\"verb\": \"POST\", \"uri\": \"https://api.acme.io/v1/payments\", \"headers\": {\"User-Agent\": \"Mozilla/5.0 (iPhone)\"}}, \"response\": {\"status\": 502, \"time\": 30001}}", "response.status", "between", "[500, 599]", false], ["{\"request\": {\"verb\": \"GET\", \"uri\": \"https://api.acme.io/v1/health\", \"headers\": {\"User-Agent\": \"ELB-HealthChecker/2.0\"}}, \"response\": {\"status\": 200, \"time\": 3}}", "request.headers.user-agent", "contains", "\"healthchecker\"", true], ["{\"request\": {\"verb\": \"DELETE\", \"uri\": \"https://api.acme.io/v1/users/77\"}, \"response\": {\"status\": 204, \"time\": 88}}", "REQUEST.Verb", "in", "[\"POST\", \"put\", \"delete\"]", true], ["{\"request\": {\"verb\": \"GET\", \"uri\": \"https://api.acme.io/v1/search?q=shoes\"}, \"response\": {\"status\": 200, \"time\": 1450}}", "response.time", "gt", "1000", false], ["{\"request\": {\"verb\": \"GET\", \"uri\": \"https://api.acme.io/v1/items\"}, \"response\": {\"status\": 200, \"body\": {\"items\": [{\"id\": \"sku-1\"}, {\"id\": \"sku-2\"}]}}}", "response.body.items.0.id", "not_equals", "\"sku-1\"", false], ["{\"user_id\": \"u-1042\", \"company_id\": \"acme\", \"metadata\": {\"plan\": \"enterprise\", \"trial\": false}}", "metadata.trial", "equals", "false", false], ["{\"user_id\": \"u-2201\", \"metadata\": {\"plan\": \"free\", \"seats\": 7}}", "metadata.seats", "lte", "\"5\"", false], ["{\"request\": {\"verb\": \"GET\", \"uri\": \"https://api.acme.io/v1/health\", \"headers\": {\"User-Agent\": \"ELB-HealthChecker/2.0\"}}, \"response\": {\"status\": 200, \"time\": 2}}", "request.headers.User-Agent", "contains", "\"healthchecker\"", false], ["{\"request\": {\"verb\": \"PUT\", \"uri\": \"https://api.acme.io/v1/carts/9\"}, \"response\": {\"status\": 404, \"time\": 41}}", "response.status", "equals", "\"200\"", false]]
# real-udf: origin=streaming id=moesif_firehose_rule_match requires= wrapped=1 ported=0
# Wrapper: case_entry builds the condition dict (operator, JSON value, ignore_case) and the event dict from JSON text, resolves the dotted path with get_path and applies OPERATORS[operator]. The sample paths all resolve and no row uses the regex operator (it needs a compiled pattern from the rule loader).
# Imports: the original module's imports.

import json
import re

# Source: https://github.com/Moesif/moesif-firehose-transform-lambda/blob/46a07ec24b2a2dc287f1b8eab3fbe0532e8e5ed4/lambda_function.py#L225-L282
# Repository: https://github.com/Moesif/moesif-firehose-transform-lambda (commit 46a07ec24b2a2dc287f1b8eab3fbe0532e8e5ed4)
# License: Apache-2.0
# System: AWS Lambda (Kinesis/Firehose)
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

MISSING = object()  # Distinguishes an absent field from a JSON null


OPERATORS = {
    "equals": lambda c, a: _equals(a, c["value"], c["ignore_case"]),
    "not_equals": lambda c, a: not _equals(a, c["value"], c["ignore_case"]),
    "in": lambda c, a: any(_equals(a, v, c["ignore_case"]) for v in c["value"]),
    "not_in": lambda c, a: not any(_equals(a, v, c["ignore_case"]) for v in c["value"]),
    "regex": lambda c, a: c["regex"].search(_text(a, False)) is not None,
    "contains": lambda c, a: _text(c["value"], c["ignore_case"]) in _text(a, c["ignore_case"]),
    "gt": lambda c, a: _compare(a, c["value"], lambda x, y: x > y),
    "gte": lambda c, a: _compare(a, c["value"], lambda x, y: x >= y),
    "lt": lambda c, a: _compare(a, c["value"], lambda x, y: x < y),
    "lte": lambda c, a: _compare(a, c["value"], lambda x, y: x <= y),
    "between": lambda c, a: _between(a, c["value"]),
    "exists": lambda c, a: True,   # Presence is checked before dispatch
    "not_exists": lambda c, a: False,
}


def get_path(payload, path):
    """Resolve a dotted path. Keys match exactly first, then case-insensitively."""
    current = payload
    for segment in path.split("."):
        if isinstance(current, dict):
            if segment in current:
                current = current[segment]
                continue
            key = next((k for k in current
                        if isinstance(k, str) and k.casefold() == segment.casefold()), MISSING)
            if key is MISSING:
                return MISSING
            current = current[key]
        elif isinstance(current, list):
            try:
                current = current[int(segment)]
            except (ValueError, IndexError):
                return MISSING
        else:
            return MISSING
    return current


def _number(value):
    """Float, or None if not numeric. Booleans are not treated as numbers."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text(value, ignore_case):
    if value is True or value is False or value is None:
        value = json.dumps(value)  # Compare using JSON spelling: true/false/null
    text = value if isinstance(value, str) else str(value)
    return text.casefold() if ignore_case else text


def _equals(actual, expected, ignore_case):
    left, right = _number(actual), _number(expected)
    if left is not None and right is not None:
        return left == right  # Numeric when both sides are numeric: 200 matches "200"
    return _text(actual, ignore_case) == _text(expected, ignore_case)


def _compare(actual, expected, test):
    left, right = _number(actual), _number(expected)
    return left is not None and right is not None and test(left, right)


def _between(actual, bounds):
    value, low, high = _number(actual), _number(bounds[0]), _number(bounds[1])
    if value is None or low is None or high is None:
        return False
    low, high = min(low, high), max(low, high)
    return low <= value <= high  # Inclusive, so [200, 299] covers all 2xx


def case_entry(event_json, path, operator, value_json, ignore_case):
    condition = {"path": path, "operator": operator, "value": json.loads(value_json), "ignore_case": ignore_case}
    actual = get_path(json.loads(event_json), path)
    return OPERATORS[operator](condition, actual)
