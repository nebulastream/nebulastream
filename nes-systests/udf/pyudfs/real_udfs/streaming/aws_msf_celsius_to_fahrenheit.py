# codon-fuzz: entry=celsius_to_fahrenheit args=FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[21.5], [-40.0], [0.0], [37.0], [100.0], [18.25], [-12.7]]
# real-udf: origin=streaming id=aws_msf_celsius_to_fahrenheit requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.
# Imports: the original module's imports.

import logging

# Source: https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples/blob/2153f99d42f2e26d030adcfe296b0dfefca9898f/python/UDF/main.py#L103-L112
# Repository: https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples (commit 2153f99d42f2e26d030adcfe296b0dfefca9898f)
# License: MIT-0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def celsius_to_fahrenheit(celsius):
    # The actual body of the UDF. It can be arbitrarily complicated.
    fahrenheit = celsius * 9 / 5 + 32

    # Logging here is for demonstration only.
    # Logging in a UDF is NOT recommended. Tt can have a substantial performance impact
    logging.debug("Celsius {} = Fahrenheit {}".format(celsius, fahrenheit))

    return fahrenheit
