# codon-fuzz: entry=unix2human args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[1514764800], [1609459199], [1700000000], [0], [1467331200], [1583020800]]
# real-udf: origin=github-spark id=cmsspark_unix2human requires= wrapped=0 ported=0
# Source: https://github.com/dmwm/CMSSpark/blob/7f32910b2f199bf03553a28d8eacc977b540f966/src/python/CMSSpark/degraded/phedex.py#L34-L36
# Repository: dmwm/CMSSpark @ commit 7f32910b2f199bf03553a28d8eacc977b540f966
# License: MIT
# Copied verbatim from src/python/CMSSpark/degraded/phedex.py (L34-L36); only the module's own import lines that the code uses are prepended.

import time


def unix2human(tstamp):
    "Convert unix time stamp into human readable format"
    return time.strftime('%Y%m%d', time.gmtime(tstamp))
