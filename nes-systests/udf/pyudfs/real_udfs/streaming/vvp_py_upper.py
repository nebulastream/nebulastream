# codon-fuzz: entry=py_upper args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["hello world"], ["straße"], ["flink-sql 1.18"], ["Ünïcödé"], [""], ["ǆemal"]]
# real-udf: origin=streaming id=vvp_py_upper requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.

# Source: https://github.com/ververica/lab-vvp-pyflink/blob/5cb7ae7d0c1bc48015483fe4afb7b281edad7cd5/python-udf/my_udfs.py#L23-L26
# Repository: https://github.com/ververica/lab-vvp-pyflink (commit 5cb7ae7d0c1bc48015483fe4afb7b281edad7cd5)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def py_upper(str):
    "This capitalizes the whole string"
    return str.upper()
