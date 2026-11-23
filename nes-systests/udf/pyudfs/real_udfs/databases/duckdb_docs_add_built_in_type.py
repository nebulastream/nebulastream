# codon-fuzz: entry=add_built_in_type args=INT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0], [41], [-1], [999999], [2147483647], [-42]]
# real-udf: origin=databases id=duckdb_docs_add_built_in_type requires= wrapped=0 ported=0
# Source: https://github.com/duckdb/duckdb-web/blob/19251f691a91ba168bd6fb610983ef82ddc2f1c9/_posts/2023-07-07-python-udf.md#L218
# Commit: 19251f691a91ba168bd6fb610983ef82ddc2f1c9
# License: MIT (duckdb/duckdb-web repository)

# Built-In UDF
def add_built_in_type(x):
    return x + 1
