# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["jane.doe@example.com"], ["078-05-1120"], ["+1 415 555 0132"], ["Maria Garcia"], ["4111 1111 1111 1111"], ["1985-07-23"]]
# real-udf: origin=github-spark id=cape_tokenizer_to_token requires= wrapped=1 ported=0
# Wrapper: _to_token is a method of Cape's Tokenizer; case_entry passes an object with a fixed 32-byte key
# Wrapper: and max_token_len=32 as the tokenizer configuration.
# Source: https://github.com/capeprivacy/cape-dataframes/blob/ed65cece5caebcce1ac549573514834effab5ecd/cape_dataframes/spark/transformations/tokenizer.py#L55-L59
# Repository: capeprivacy/cape-dataframes @ commit ed65cece5caebcce1ac549573514834effab5ecd
# License: Apache-2.0
# Copied verbatim from cape_dataframes/spark/transformations/tokenizer.py (L55-L59); only the module's own import lines that the code uses are prepended.
# Applied per value through: spark-pandas_udf

import hashlib


def _to_token(self, x: str):
    token = hashlib.sha256(x.encode() + self.key).hexdigest()
    if self._max_token_len is None:
        return token
    return token[: self._max_token_len]


class _TokenizerConfig:
    key = bytes.fromhex("8f2c1e6b5a4d3c2b1a09f8e7d6c5b4a39281706f5e4d3c2b1a0918273645a5b6")
    _max_token_len = 32


def case_entry(x):
    return _to_token(_TokenizerConfig(), x)
