# codon-fuzz: entry=length args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Hello"], ["Hello Flink"], [""], ["Grüße"], ["https://flink.apache.org/what-is-flink/flink-architecture/"], ["日本語テキスト"]]
# real-udf: origin=streaming id=pyflink_length requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.

# Source: https://github.com/apache/flink/blob/b1f051d09c4111898902167428199c00747a5076/flink-python/pyflink/examples/table/mixing_use_of_datastream_and_table.py#L53-L55
# Repository: https://github.com/apache/flink (commit b1f051d09c4111898902167428199c00747a5076)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def length(data):
    return len(data)
