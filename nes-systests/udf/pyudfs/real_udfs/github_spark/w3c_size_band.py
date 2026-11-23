# codon-fuzz: entry=size_band args=INT64,INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0, 0], [512, 300], [4096, 1200], [50000, 800], [700000, 2000], [5242880, 400], [1023, 0]]
# real-udf: origin=github-spark id=w3c_size_band requires= wrapped=0 ported=0
# Source: https://github.com/AhmedIkram05/w3c-etl-pipeline/blob/9b30e751e8f4dadd4c9eca2e26628b7f30a123bd/pipeline/spark/jobs/utils/transformations.py#L201-L217
# Repository: AhmedIkram05/w3c-etl-pipeline @ commit 9b30e751e8f4dadd4c9eca2e26628b7f30a123bd
# License: MIT
# Copied verbatim from pipeline/spark/jobs/utils/transformations.py (L192-L198, L201-L217); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

_SIZE_BANDS = [
    (1, "Zero"),  # total bytes == 0
    (1024, "Tiny"),  # 1 - 1023
    (10240, "Small"),  # 1024 - 10239
    (102400, "Medium"),  # 10240 - 102399
    (1048576, "Large"),  # 102400 - 1048575
]


def size_band(bytes_sent, bytes_recv):
    """Bucket total bytes (sent + received) into a human-readable band.

    Thresholds (total bytes):
        = 0      → ``Zero``
        1-1023   → ``Tiny``
        1024-10239    → ``Small``
        10240-102399  → ``Medium``
        102400-1048575 → ``Large``
        >= 1048576    → ``Huge``
    """
    total = (bytes_sent or 0) + (bytes_recv or 0)
    for threshold, label in _SIZE_BANDS:
        if total < threshold:
            return label
    return "Huge"
