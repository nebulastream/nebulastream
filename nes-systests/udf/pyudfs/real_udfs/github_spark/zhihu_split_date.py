# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["2018-03-15T08:30:00"], ["2017-12-31T23:59:59"], ["2019-02-28T00:00:01"], ["2016-02-29T12:00:00"], ["2020-07-04T18:45:30"]]
# real-udf: origin=github-spark id=zhihu_split_date requires= wrapped=1 ported=0
# Wrapper: the UDF returns a tuple (registered as an array); case_entry returns it as JSON text.
# Source: https://github.com/mikolaje/zhihu_display/blob/40b11618e2d546b6bbb72104f9a56554e58bfe36/spark_script/analyze_answers.py#L28-L34
# Repository: mikolaje/zhihu_display @ commit 40b11618e2d546b6bbb72104f9a56554e58bfe36
# License: MIT
# Copied verbatim from spark_script/analyze_answers.py (L28-L34); only the module's own import lines that the code uses are prepended.

import datetime


def split_date(_datetime):
    _datetime = datetime.datetime.strptime(_datetime, '%Y-%m-%dT%H:%M:%S')

    ret = _datetime.year, _datetime.month, _datetime.day, \
            _datetime.hour, _datetime.minute, _datetime.second, _datetime.weekday(), str(_datetime.date())

    return ret


import json


def case_entry(_datetime):
    return json.dumps(split_date(_datetime))
