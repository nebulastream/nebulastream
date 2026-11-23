# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["49590338271490256608559692538361571095921575989136580001", "eyJBdG9taWNOdW1iZXIiOiAxLCAiRWxlbWVudCI6ICJIeWRyb2dlbiIsICJTeW1ib2wiOiAiSCIsICJBdG9taWNNYXNzIjogMS4wMDh9"], ["49590338271490256608559692538361571095921575989136580008", "eyJBdG9taWNOdW1iZXIiOiA4LCAiRWxlbWVudCI6ICJPeHlnZW4iLCAiU3ltYm9sIjogIk8iLCAiQXRvbWljTWFzcyI6IDE1Ljk5OX0="], ["49590338271490256608559692538361571095921575989136580015", "eyJBdG9taWNOdW1iZXIiOiAyNiwgIkVsZW1lbnQiOiAiSXJvbiIsICJTeW1ib2wiOiAiRmUiLCAiQXRvbWljTWFzcyI6IDU1Ljg0NX0="], ["49590338271490256608559692538361571095921575989136580022", "eyJBdG9taWNOdW1iZXIiOiA2LCAiRWxlbWVudCI6ICJDYXJib24iLCAiQXRvbWljTWFzcyI6IDEyLjAxMX0="], ["49590338271490256608559692538361571095921575989136580029", "eyJBdG9taWNOdW1iZXIiOiAiNzkiLCAiRWxlbWVudCI6ICJHb2xkIiwgIlN5bWJvbCI6ICJBdSIsICJBdG9taWNNYXNzIjogMTk2Ljk3fQ=="], ["49590338271490256608559692538361571095921575989136580036", "eyJBdG9taWNOdW1iZXIiOiAyLCAiRWxlbWVudCI6ICJIZWxpdW0iLCAiU3ltYm9sIjogIkhlIiwgIkF0b21pY01hc3MiOiA0fQ=="], ["49590338271490256608559692538361571095921575989136580043", "W3siQXRvbWljTnVtYmVyIjogMywgIkVsZW1lbnQiOiAiTGl0aGl1bSIsICJTeW1ib2wiOiAiTGkiLCAiQXRvbWljTWFzcyI6IDYuOTR9XQ=="]]
# real-udf: origin=streaming id=aws_kinesis_validate_fields requires= wrapped=1 ported=0
# Wrapper: case_entry builds the Kinesis event with one record (sequence number, base64 data) and calls the Lambda handler; the handler returns nothing or raises ValueError, so case_entry returns "valid" or the error text.
# Imports: the original module's imports and its module-level logger.

import base64
import json
import logging

logger = logging.getLogger()

# Source: https://github.com/aws-samples/serverless-patterns/blob/01b933c9aa1d4cf933239621ba3e508269c69237/kinesis-lambda-error-handling/python/sam/kinesis-consumer/kinesis-consumer-fn.py#L18-L46
# Repository: https://github.com/aws-samples/serverless-patterns (commit 01b933c9aa1d4cf933239621ba3e508269c69237)
# License: MIT-0
# System: AWS Lambda (Kinesis/Firehose)
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def lambda_handler(event, context):
    # Record/Data should match the below Keys and the data type
    required_fields = {"AtomicNumber": int, "Element": str, "Symbol": str, "AtomicMass": float}
    logger.info(f"Incoming event: -->  {event}")

    # Variable to print the Unique sequence of each record
    curRecordSequenceNumber = ""

    # Loop through the Records to read each record
    for record in event['Records']:
        curRecordSequenceNumber = record["kinesis"]["sequenceNumber"]
        logger.info(f"Sequence Number of the current record --> {curRecordSequenceNumber}")

        # Convert the base64 data into utf before validating for the schema
        payload = json.loads(base64.b64decode(record['kinesis']['data']).decode('utf-8'))
        logger.info(f"Individual record content --> {payload}")
        if not isinstance(payload, dict):
            logger.info("Invalid JSON Data Structure.The parsed data does not adhere to the expeced JSON data structure.")
            raise ValueError("Invalid JSON Data Structure",
                             "The parsed data does not adhere to the expeced JSON data structure.")

        # Verify if the key, value are as per expectations    
        for key, value_type in required_fields.items():
            if key not in payload:
                logger.info(f"Missing '{key}' field in JSON.")
                raise ValueError(f"Missing '{key}' field in JSON.")
            if not isinstance(payload[key], value_type):
                logger.info(f"'{key}' field should be of type {value_type.__name__}.")
                raise ValueError(f"'{key}' field should be of type {value_type.__name__}.") 


def case_entry(sequence_number, data):
    event = {"Records": [{"kinesis": {"sequenceNumber": sequence_number, "data": data}}]}
    try:
        lambda_handler(event, None)
    except ValueError as error:
        return str(error)
    return "valid"
