# codon-fuzz: entry=norm args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["山田　太郎"], ["ＹＡＭＡＤＡ Taro"], ["ｻﾄｳ ﾊﾅｺ"], ["  Suzuki\tIchiro  "], ["髙橋 一郎"], ["佐藤 花子 (ｻﾄｳ)"]]
# real-udf: origin=github-spark id=ykst_norm requires= wrapped=0 ported=0
# Source: https://github.com/yk-st/data-platform-on-local/blob/517f9333309eef0cf7e60f82bbd70b36b88707cd/working/programs/spark/name_collection/deterministic.py#L37-L40
# Repository: yk-st/data-platform-on-local @ commit 517f9333309eef0cf7e60f82bbd70b36b88707cd
# License: MIT
# Copied verbatim from working/programs/spark/name_collection/deterministic.py (L37-L40); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf("string")

import re, unicodedata


def norm(s):
    if s is None: return ""
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", s).lower())
