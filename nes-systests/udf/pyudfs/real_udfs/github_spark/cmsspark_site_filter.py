# codon-fuzz: entry=site_filter args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["T1_US_FNAL_MSS"], ["T1_US_FNAL_Disk"], ["T0_CH_CERN_Export"], ["T1_DE_KIT_Buffer"], ["T2_CH_CERN"], ["T2_US_Wisconsin"], ["T1_UK_RAL_MSS"]]
# real-udf: origin=github-spark id=cmsspark_site_filter requires= wrapped=0 ported=0
# Source: https://github.com/dmwm/CMSSpark/blob/7f32910b2f199bf03553a28d8eacc977b540f966/src/python/CMSSpark/degraded/phedex.py#L38-L42
# Repository: dmwm/CMSSpark @ commit 7f32910b2f199bf03553a28d8eacc977b540f966
# License: MIT
# Copied verbatim from src/python/CMSSpark/degraded/phedex.py (L38-L42); only the module's own import lines that the code uses are prepended.

def site_filter(site):
    "Filter site names without _MSS or _Buffer or _Export"
    if site.endswith('_MSS') or site.endswith('_Buffer') or site.endswith('_Export'):
        return 0
    return 1
