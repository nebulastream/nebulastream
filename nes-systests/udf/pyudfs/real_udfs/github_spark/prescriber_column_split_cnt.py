# codon-fuzz: entry=column_split_cnt args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["12 years"], ["3"], ["10 years 6 months"], ["over 25 years of experience"], ["less than one year"]]
# real-udf: origin=github-spark id=prescriber_column_split_cnt requires= wrapped=0 ported=0
# Source: https://github.com/judeleonard/Prescriber-ETL-data-pipeline/blob/d2daeb51a6d89990c674f454f276ced4461883ab/scripts/spark_etl.py#L59-L61
# Repository: judeleonard/Prescriber-ETL-data-pipeline @ commit d2daeb51a6d89990c674f454f276ced4461883ab
# License: Apache-2.0
# Copied verbatim from scripts/spark_etl.py (L59-L61); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=IntegerType())

def column_split_cnt(column):
    return len(column.split(' '))
