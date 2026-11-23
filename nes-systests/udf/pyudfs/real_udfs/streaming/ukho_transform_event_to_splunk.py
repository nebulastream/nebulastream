# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1716202800123", "{\"requestId\": \"c6af9ac6-7b61-11e6-9a41-93e8deadbeef\", \"ip\": \"203.0.113.24\", \"httpMethod\": \"POST\", \"resourcePath\": \"/login\", \"status\": 200, \"body\": {\"user\": \"alice\", \"password\": \"hunter2\"}}", "a1b2c3d4e5f6/2024/05/20"], ["1716202801456", "2024-05-20T10:21:44.120Z INFO  GET /api/orders/1042 Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.sig status=200", "orders-api/prod/7f3c"], ["1716202802789", "2024-05-20T10:21:45.001Z INFO  GET /health 200 ELB-HealthChecker/2.0", "orders-api/prod/7f3c"], ["1716202803000", "{\"level\": \"ERROR\", \"message\": \"upstream timeout\", \"latency_ms\": 30001, \"route\": \"/api/payments\"}", ""], ["1716202804321", "2024-05-20T10:21:47.333Z WARN  retrying request to https://payments.internal/charge attempt=2", "payments/prod/19aa"], ["1716202805999", "{\"event\": \"login\", \"user\": \"bob\", \"password\": \"correct horse battery staple\", \"mfa\": true}", "auth/prod/02bc"]]
# real-udf: origin=streaming id=ukho_transform_event_to_splunk requires= wrapped=1 ported=0
# Wrapper: case_entry passes a constant sourcetype configuration (a denylist regex and two redact regexes) and constant index, sourcetype name, Firehose ARN, account and source; a dropped log (None) becomes "".
# Imports: the original module's imports, its module-level logger and DEFAULT_ALLOW_REGEX (handler.py line 209).

import base64
import gzip
import json
import logging
import re

logger = logging.getLogger()
DEFAULT_ALLOW_REGEX = ".*"

# Source: https://github.com/UKHomeOffice/firehose-splunk-tf-module/blob/e9443359f36a8ce8f68390f2af0abecd9232e864/lambdas/transformation_lambda/src/mbtp_splunk_cloudwatch_transformation/handler.py#L212-L337
# Repository: https://github.com/UKHomeOffice/firehose-splunk-tf-module (commit e9443359f36a8ce8f68390f2af0abecd9232e864)
# License: MIT
# System: AWS Lambda (Kinesis/Firehose)
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def is_json(j: str) -> bool:
    """Checks if a string is json parsable

    Args:
        j (str): JSON to test

    Returns:
        bool: True if string is json parsable
    """
    try:
        json.loads(j)
    except ValueError:
        return False
    return True


def get_record_type(data: dict) -> str | None:
    """Takes data from a firehose record and works out what type of event it is

    Args:
        data (dict): Firehose record data

    Returns:
        str | None: Type of record
    """
    keys = data.keys()
    if set(("messageType", "logGroup", "owner", "logEvents")) <= keys:
        return "cloudwatch"
    elif set(("source", "detail-type")) <= keys:
        return "eventbridge"
    elif set(("index", "sourcetype", "event")) <= keys:
        return "splunk"
    return None


def load_firehose_record_data(base64_data: str) -> dict:
    """Converts a b64, optionally gzip compressed, string into a dictionary

    Args:
        base64_data (str): Base64 encoded data

    Returns:
        dict: Decoded, maybe decompressed, loaded dictionary
    """
    data = base64.b64decode(base64_data)
    try:
        return json.loads(gzip.decompress(data))
    except gzip.BadGzipFile:
        return json.loads(data)


def transform_event_to_splunk(
    timestamp: str,
    event: str | dict,
    index: str,
    sourcetype: dict,
    sourcetype_name: str,
    firehose_arn: str,
    account_id: str,
    source: str,
    log_stream: str | None = None,
) -> None | str:
    """Transforms an event into a Splunk one

    Args:
        timestamp (str): Event timestamp
        event (str | dict): Initial event
        index (str): Splunk Index
        sourcetype (dict): Sourcetype object containing regexes
        sourcetype_name (str): Splunk Sourcetype name
        firehose_arn (str): Firehose ARN
        account_id (str): AWS Account ID of the initial log
        source (str): Source of the event
        log_stream (str | None, optional): Log group stream. Defaults to None.

    Returns:
        None | str: The processed message or None if we dropped it
    """

    log = json.dumps(event) if isinstance(event, dict) else event

    for deny_regex in sourcetype.get("denylist_regexes", []):
        if re.search(deny_regex, log):
            logger.info(f"Dropping log due to matching {deny_regex}. Log = {log}")
            return None

    allowed = False
    for allow_regex in sourcetype.get("allowlist_regexes", [DEFAULT_ALLOW_REGEX]):
        if re.search(allow_regex, log):
            allowed = True
            break
    if not allowed:
        logger.info(
            f"Dropping log because it did not match any allowlist regexes. Log = {log}"
        )
        return None

    for regex_index, redact_regex in enumerate(sourcetype.get("redact_regexes", [])):
        if match := re.search(redact_regex, log):
            for group_index, _ in enumerate(match.groups()):
                group_span = match.span(group_index + 1)
                log = (
                    log[: group_span[0]]
                    + f"***REDACTED BY {sourcetype_name}.redact_regexes.{regex_index}***"
                    + log[group_span[1] :]
                )

    if is_json(log):
        log = json.loads(log)

    new_log = {
        "index": index,
        "sourcetype": sourcetype_name,
        "time": timestamp,
        "host": firehose_arn,
        "source": source,
        "fields": {
            "aws_account_id": account_id,
        },
        "event": log,
    }

    if log_stream:
        new_log["fields"]["cw_log_stream"] = log_stream

    return json.dumps(new_log)


SOURCETYPE = {
    "denylist_regexes": [r"ELB-HealthChecker"],
    "redact_regexes": [r'"password": "([^"]*)"', r"Authorization: Bearer (\S+)"],
}


def case_entry(timestamp, event, log_stream):
    result = transform_event_to_splunk(
        timestamp,
        event,
        "aws_cloudwatch",
        SOURCETYPE,
        "aws:cloudwatch:apigateway",
        "arn:aws:firehose:eu-west-2:123456789012:deliverystream/splunk-cloudwatch",
        "123456789012",
        "/aws/apigateway/orders-api",
        log_stream,
    )
    return "" if result is None else result
