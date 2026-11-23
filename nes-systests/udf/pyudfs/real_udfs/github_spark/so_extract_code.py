# codon-fuzz: entry=extract_code args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["<p>Try this:</p><pre><code>df.groupby('a').sum()</code></pre>"], ["<p>No code here.</p>"], ["<p>Use <code>pip install pandas</code> then</p><pre><code>import pandas as pd\nprint(pd.__version__)\n</code></pre>"], ["<code>x = 1</code> and <code>y = 2</code>"], ["<p>See <a href=\"https://docs.python.org\">docs</a></p>"]]
# real-udf: origin=github-spark id=so_extract_code requires= wrapped=0 ported=0
# Source: https://github.com/rehanalfarizu/stackoverflow-analytics-pyspark-nlp/blob/3385401530ef2db07e3727550aff00214e74f292/src/etl/data_transformer.py#L69-L74
# Repository: rehanalfarizu/stackoverflow-analytics-pyspark-nlp @ commit 3385401530ef2db07e3727550aff00214e74f292
# License: MIT
# Copied verbatim from src/etl/data_transformer.py (L69-L74); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(returnType=StringType())

import re


def extract_code(text):
    if text is None:
        return ""
    code_blocks = re.findall(r'<code>(.*?)</code>', text, re.DOTALL)
    return '\n'.join(code_blocks)
