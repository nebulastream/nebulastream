# codon-fuzz: entry=udf args=INT64 returns=VARSIZED

import json


def udf(n):
    return json.dumps(n / 3)
