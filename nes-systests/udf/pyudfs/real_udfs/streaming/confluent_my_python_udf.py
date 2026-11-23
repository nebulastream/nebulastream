# codon-fuzz: entry=my_python_udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["order_created"], ["user-42"], ["hello world"], ["Grüße aus Straßburg"], ["sensor_7:temp=21.5"], [""]]
# real-udf: origin=streaming id=confluent_my_python_udf requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.

# Source: https://github.com/confluentinc/flink-test-suite/blob/8df6f4c802f6e188e49321291872fe6f9d86f18c/examples/udf/python/my_udfs.py#L4-L7
# Repository: https://github.com/confluentinc/flink-test-suite (commit 8df6f4c802f6e188e49321291872fe6f9d86f18c)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def my_python_udf(input_str):
    # Your UDF logic here
    return input_str.upper() + "_processed"
