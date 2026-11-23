# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["4954698668313554428650745793632162567570019247115678515400", "eyJzb3VyY2V0eXBlIjogImF3czpjbG91ZHRyYWlsIiwgInNvdXJjZSI6ICJhd3NfZmlyZWhvc2VfY2xvdWR0cmFpbCIsICJ0aW1lIjogMTcxNjIwMjgwMC4xMjMsICJldmVudCI6IHsiZXZlbnROYW1lIjogIkNvbnNvbGVMb2dpbiIsICJhd3NSZWdpb24iOiAiZXUtd2VzdC0xIiwgInNvdXJjZUlQQWRkcmVzcyI6ICIyMDMuMC4xMTMuMjQifX0="], ["4954698668313554428650745793632162567570019247115678515401", "eyJzb3VyY2V0eXBlIjogImFjY2Vzc19jb21iaW5lZCIsICJzb3VyY2UiOiAiL3Zhci9sb2cvbmdpbngvYWNjZXNzLmxvZyIsICJldmVudCI6ICIxOTguNTEuMTAwLjcgLSAtIFsyMC9NYXkvMjAyNDoxMDoyMTo0NCArMDAwMF0gXCJHRVQgL2FwaS9vcmRlcnMvMTA0MiBIVFRQLzEuMVwiIDIwMCA1MTIgXCItXCIgXCJjdXJsLzguNS4wXCIifQ=="], ["4954698668313554428650745793632162567570019247115678515402", "eyJzb3VyY2V0eXBlIjogImF3czpjbG91ZHdhdGNobG9ncyIsICJzb3VyY2UiOiAiL2F3cy9sYW1iZGEvY2hlY2tvdXQiLCAidGltZSI6ICIxNzE2MjAyODYxIiwgImV2ZW50IjogIlNUQVJUIFJlcXVlc3RJZDogNmYxYzJhMWUtOGQ0Yi00ZjBlLTlhNTEtMGMyYTVkM2U3YjExIFZlcnNpb246ICRMQVRFU1QiLCAiZmllbGRzIjogeyJyZWdpb24iOiAidXMtZWFzdC0xIiwgImFjY291bnQiOiAiMTIzNDU2Nzg5MDEyIn19"], ["4954698668313554428650745793632162567570019247115678515403", "eyJzb3VyY2V0eXBlIjogInN5c2xvZyIsICJzb3VyY2UiOiAidWRwOjUxNCIsICJ0aW1lIjogMTcxNjIwMjkwMCwgImV2ZW50IjogInNzaGRbMjIxMV06IEFjY2VwdGVkIHB1YmxpY2tleSBmb3IgZWMyLXVzZXIgZnJvbSAxMC4wLjQuMTcgcG9ydCA1MDEyMiIsICJmaWVsZHMiOiB7Imhvc3QiOiAiaXAtMTAtMC00LTE3In19"], ["4954698668313554428650745793632162567570019247115678515404", "eyJzb3VyY2V0eXBlIjogImpzb24iLCAic291cmNlIjogImFwcDpwYXltZW50cyIsICJldmVudCI6IHsibGV2ZWwiOiAiRVJST1IiLCAibXNnIjogImNhcmQgZGVjbGluZWQiLCAiYW1vdW50IjogNDkuOTksICJjdXJyZW5jeSI6ICJFVVIifX0="]]
# real-udf: origin=streaming id=splunk_firehose_process_records requires= wrapped=1 ported=0
# Wrapper: case_entry builds the list with one Firehose record (recordId, base64 data) and returns the records that the processRecords generator yields as JSON text.
# Imports: the original module's imports.

import base64
import json
import sys

# Source: https://github.com/pauld-splunk/aws-splunk-firehose-error-reingest/blob/a0dc898eb7f27539effb95481a90d3f8dd2822c3/firehose-reingest/kinesis_lambda_function.py#L14-L39
# Repository: https://github.com/pauld-splunk/aws-splunk-firehose-error-reingest (commit a0dc898eb7f27539effb95481a90d3f8dd2822c3)
# License: Apache-2.0
# System: AWS Lambda (Kinesis/Firehose)
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

IS_PY3 = sys.version_info[0] == 3


def processRecords(records):

    for r in records:

        data = json.loads(base64.b64decode(r['data']))
        recId = r['recordId']
        return_event = {}

        return_event['sourcetype'] = data['sourcetype']
        return_event['source'] = data['source']
        return_event['event'] = data['event']
        if data.get('time')!=None:
            return_event['time'] = data['time']
        if data.get('fields')!=None:
            return_event['fields'] = data['fields']

        if IS_PY3:
            # base64 encode api changes in python3 to operate exclusively on byte-like objects and bytes
            data = base64.b64encode(json.dumps(return_event).encode('utf-8')).decode()
        else:
            data = base64.b64encode(json.dumps(return_event))
        yield {
            'data': data,
            'result': 'Ok',
            'recordId': recId
        }


def case_entry(record_id, data):
    return json.dumps(list(processRecords([{"recordId": record_id, "data": data}])), sort_keys=True)
