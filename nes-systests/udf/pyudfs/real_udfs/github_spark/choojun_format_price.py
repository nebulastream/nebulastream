# codon-fuzz: entry=format_price args=FLOAT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[12.5], [1234.5], [999999.999], [0.0], [45.678], [1500000.0], [88.005]]
# real-udf: origin=github-spark id=choojun_format_price requires= wrapped=0 ported=0
# Source: https://github.com/choojun/de/blob/76d082577f444400a007961424f1db1fc5bee8c0/sales_processor.py#L7-L10
# Repository: choojun/de @ commit 76d082577f444400a007961424f1db1fc5bee8c0
# License: MIT
# Copied verbatim from sales_processor.py (L7-L10); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @staticmethod; @udf(returnType=StringType())

def format_price(price):
    return f"RM{price:,.2f}"
