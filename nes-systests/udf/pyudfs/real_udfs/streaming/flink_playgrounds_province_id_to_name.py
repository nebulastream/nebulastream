# codon-fuzz: entry=province_id_to_name args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0], [1], [2], [3], [4], [5], [6]]
# real-udf: origin=streaming id=flink_playgrounds_province_id_to_name requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic. The decorator declares a STRING input, but the code indexes a tuple, so the argument is INT64 (the provinceId column of the walkthrough is an INT).

# Source: https://github.com/apache/flink-playgrounds/blob/2117c5ea8cf31d39237d16cc9dee5507544761fc/pyflink-walkthrough/payment_msg_proccessing.py#L28-L30
# Repository: https://github.com/apache/flink-playgrounds (commit 2117c5ea8cf31d39237d16cc9dee5507544761fc)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

provinces = ("Beijing", "Shanghai", "Hangzhou", "Shenzhen", "Jiangxi", "Chongqing", "Xizang")


def province_id_to_name(id):
    return provinces[id]
