# codon-fuzz: entry=remove_html args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["<p>How do I <strong>merge</strong> two dicts?</p>"], ["<p>Line one</p>\n\n<p>Line   two</p>"], ["plain text"], ["<pre><code>a &lt; b</code></pre>"], ["<a href=\"https://stackoverflow.com\">link</a> text"]]
# real-udf: origin=github-spark id=so_remove_html requires= wrapped=0 ported=0
# Source: https://github.com/rehanalfarizu/stackoverflow-analytics-pyspark-nlp/blob/3385401530ef2db07e3727550aff00214e74f292/src/etl/data_transformer.py#L58-L64
# Repository: rehanalfarizu/stackoverflow-analytics-pyspark-nlp @ commit 3385401530ef2db07e3727550aff00214e74f292
# License: MIT
# Copied verbatim from src/etl/data_transformer.py (L58-L64); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(returnType=StringType())

import re


def remove_html(text):
    if text is None:
        return ""
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'\s+', ' ', clean)
    return clean.strip()
