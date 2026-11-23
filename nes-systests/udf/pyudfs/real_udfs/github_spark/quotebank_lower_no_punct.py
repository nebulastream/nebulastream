# codon-fuzz: entry=lower args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Hello, World!"], ["It's a \"great\" day -- isn't it?"], ["Mr. Smith (CEO) said: 'No comment.'"], ["Résumé — naïve café"], ["   multiple   spaces\tand\ttabs  "]]
# real-udf: origin=github-spark id=quotebank_lower_no_punct requires= wrapped=0 ported=0
# Source: https://github.com/epfl-dlab/Quotebank/blob/a203acf5a05e34841671578d14d6a0be6a66a3ef/quobert/dataprocessing/postprocessing/process_res.py#L12-L16
# Repository: epfl-dlab/Quotebank @ commit a203acf5a05e34841671578d14d6a0be6a66a3ef
# License: MIT
# Copied verbatim from quobert/dataprocessing/postprocessing/process_res.py (L12-L16); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf

import re
import string


def lower(text):
    no_punct = "".join(t for t in text if t not in string.punctuation)
    one_space = re.sub(r"\W+", " ", no_punct)
    return one_space.lower()
