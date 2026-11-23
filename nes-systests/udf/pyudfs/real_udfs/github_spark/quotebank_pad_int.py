# codon-fuzz: entry=pad_int args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[7], [42], [123456], [0], [1234567], [999]]
# real-udf: origin=github-spark id=quotebank_pad_int requires= wrapped=0 ported=0
# Source: https://github.com/epfl-dlab/Quotebank/blob/a203acf5a05e34841671578d14d6a0be6a66a3ef/quobert/dataprocessing/postprocessing/process_res.py#L24-L26
# Repository: epfl-dlab/Quotebank @ commit a203acf5a05e34841671578d14d6a0be6a66a3ef
# License: MIT
# Copied verbatim from quobert/dataprocessing/postprocessing/process_res.py (L24-L26); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf

def pad_int(idx):
    return f"{idx:06d}"
