# codon-fuzz: entry=add_built_in_type args=INT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0], [1], [41], [-17], [2023], [999999], [-1]]
# real-udf: origin=literature id=duckdb_add_built_in_type requires= wrapped=0 ported=0

# Source: From Waddle to Flying: Quickly Expanding DuckDB's Functionality with Scalar Python UDFs (DuckDB blog, Holanda, 2023)
# URL: https://github.com/duckdb/duckdb-web/blob/7d751a272ab6fe1cb40c4d2f1d1a5c2daa41495b/_posts/2023-07-07-python-udf.md#L217-L219
# Repository: https://github.com/duckdb/duckdb-web at commit 7d751a272ab6fe1cb40c4d2f1d1a5c2daa41495b
# License: MIT
# Path: _posts/2023-07-07-python-udf.md, lines 217-219

# Built-In UDF
def add_built_in_type(x):
    return x + 1
