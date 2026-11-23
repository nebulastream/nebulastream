# codon-fuzz: entry=contains_flink args=VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Hello Flink"], ["flink lowercase"], ["Apache Flink 1.20 released"], ["Spark Structured Streaming"], [""], ["PyFlink Table API"]]
# real-udf: origin=streaming id=pyflink_contains_flink requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.

# Source: https://github.com/apache/flink/blob/b1f051d09c4111898902167428199c00747a5076/flink-python/pyflink/examples/table/multi_sink.py#L58-L60
# Repository: https://github.com/apache/flink (commit b1f051d09c4111898902167428199c00747a5076)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def contains_flink(data):
    return "Flink" in data
