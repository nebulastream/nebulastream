# codon-fuzz: entry=to_fahr args=VARSIZED,FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Chicago", 22.5], ["Berlin", 18.0], ["New York", -3.2], ["Seattle", 12.0], ["Tokyo", 29.4], ["Portland", 0.0], ["chicago", 20.0]]
# real-udf: origin=streaming id=flink_cookbook_to_fahr requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.

# Source: https://github.com/ververica/flink-sql-cookbook/blob/33bef8ddd19a893403fbf979f70d651e2da6f5d4/udfs/01_python_udfs/python_udf.py#L6-L16
# Repository: https://github.com/ververica/flink-sql-cookbook (commit 33bef8ddd19a893403fbf979f70d651e2da6f5d4)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

us_cities = {"Chicago","Portland","Seattle","New York"}


def to_fahr(city, temperature):

	if city in us_cities:

		fahr = ((temperature * 9.0 / 5.0) + 32.0)

		return fahr
	else:
		return temperature
