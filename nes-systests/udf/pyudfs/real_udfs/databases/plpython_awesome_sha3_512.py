# codon-fuzz: entry=sha3_512 args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["hello world"], [""], ["jane.doe@example.com"], ["2024-03-15T14:22:05Z|order-10422|59.90"], ["Grüße aus München"], ["password123"]]
# real-udf: origin=databases id=plpython_awesome_sha3_512 requires= wrapped=0 ported=0
# Source: https://github.com/vladimir-poghosyan/awesome-plpython/blob/59bdc7f8620c6523a75d997439e4d6a8200fcbfe/scripts/sha3_512.sql#L20
# Commit: 59bdc7f8620c6523a75d997439e4d6a8200fcbfe
# License: MIT
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.

def sha3_512(data):
    from hashlib import sha3_512


    # return NULL if input is NULL
    if data is None:
        return None

    return sha3_512(data.encode()).hexdigest()
