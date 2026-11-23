# codon-fuzz: entry=world_cups args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Brazil"], ["Germany"], ["Italy"], ["Argentina"], ["Uruguay"], ["France"], ["England"], ["Spain"]]
# real-udf: origin=databases id=duckdb_docs_world_cups requires= wrapped=0 ported=0
# Source: https://github.com/duckdb/duckdb-web/blob/19251f691a91ba168bd6fb610983ef82ddc2f1c9/_posts/2023-07-07-python-udf.md#L68
# Commit: 19251f691a91ba168bd6fb610983ef82ddc2f1c9
# License: MIT (duckdb/duckdb-web repository)

world_cup_titles = {
    "Brazil": 5,
    "Germany": 4,
    "Italy": 4,
    "Argentina": 2,
    "Uruguay": 2,
    "France": 2,
    "England": 1,
    "Spain": 1
}

# Function that will be registered as an UDF, simply does a lookup in the python dictionary
def world_cups(x):
     return world_cup_titles.get(x)
