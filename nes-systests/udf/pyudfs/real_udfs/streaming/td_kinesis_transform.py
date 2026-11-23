# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["eyJ0aW1lIjogMTcxNjIwMjgwMCwgInVzZXJfaWQiOiAidS0xMDQyIiwgImV2ZW50IjogInBhZ2VfdmlldyIsICJwYXRoIjogIi9wcm9kdWN0cy80MiJ9"], ["eyJ0aW1lIjogMTcxNjIwMjgxNSwgInVzZXJfaWQiOiAidS0yMjAxIiwgImV2ZW50IjogImFkZF90b19jYXJ0IiwgInNrdSI6ICJTS1UtODgxMiIsICJxdHkiOiAyLCAicHJpY2UiOiAxOS45OX0="], ["eyJ0aW1lIjogMTcxNjIwMjgzMywgImRldmljZSI6ICJ0aGVybW9zdGF0LTE3IiwgInRlbXBlcmF0dXJlIjogMjEuNSwgImh1bWlkaXR5IjogMC40MX0="], ["eyJ0aW1lIjogMTcxNjIwMjg1MCwgInVzZXJfaWQiOiAidS0xMDQyIiwgImV2ZW50IjogImNoZWNrb3V0IiwgIml0ZW1zIjogWyJTS1UtODgxMiIsICJTS1UtMDA0MiJdLCAidG90YWwiOiA2NC45N30="], ["eyJ0aW1lIjogMTcxNjIwMjkwMSwgImxldmVsIjogIndhcm4iLCAibWVzc2FnZSI6ICJzbG93IHF1ZXJ5IiwgImR1cmF0aW9uX21zIjogMTgzMiwgInRhZ3MiOiB7ImRiIjogIm9yZGVycyJ9fQ=="]]
# real-udf: origin=streaming id=td_kinesis_transform requires= wrapped=1 ported=0
# Wrapper: case_entry builds the Kinesis record dict from its base64 data and returns the payload dict as JSON text. All sample rows carry "time", so the time.time() branch is not taken (it is not deterministic).
# Imports: the original module's imports.

import base64
import json
import time

# Source: https://github.com/treasure-data/treasure-boxes/blob/4130d6194a91ce79fa2a35d50e89a3ceed9075c7/integration-box/Kinesis/lambda_function.py#L48-L55
# Repository: https://github.com/treasure-data/treasure-boxes (commit 4130d6194a91ce79fa2a35d50e89a3ceed9075c7)
# License: MIT
# System: AWS Lambda (Kinesis/Firehose)
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def transform(record):
    # Kinesis data is base64 encoded so decode here
    # We also assume payload comes as JSON form
    # Depending on the payload format, change code below to extract exact content from JSON object
    payload = json.loads(base64.b64decode(record["data"]))
    if "time" not in payload:
        payload["time"] = int(time.time())
    return payload


def case_entry(data):
    return json.dumps(transform({"data": data}), sort_keys=True)
