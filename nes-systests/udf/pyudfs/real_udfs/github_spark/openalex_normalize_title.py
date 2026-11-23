# codon-fuzz: entry=normalize_title args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["The Effect of <i>Escherichia coli</i> on Gut Microbiota"], ["A Survey of Deep Learning for Natural Language Processing"], ["Économie et société à l'époque moderne"], ["Folder 1983: Scan 29"], ["Graduate Catalog 2009-2011"], ["On the Origin of Species by Means of Natural Selection"], ["<p>Über die Quantentheorie</p>"]]
# real-udf: origin=github-spark id=openalex_normalize_title requires= wrapped=0 ported=0
# Source: https://github.com/ourresearch/openalex-walden/blob/d727817db8c1bfa5fae5bec4103dd8bd07ce3f2f/libraries/dlt_utils/openalex/dlt/normalize.py#L205-L228
# Repository: ourresearch/openalex-walden @ commit d727817db8c1bfa5fae5bec4103dd8bd07ce3f2f
# License: MIT
# Copied verbatim from libraries/dlt_utils/openalex/dlt/normalize.py (L189-L203, L205-L228); only the module's own import lines that the code uses are prepended.
# Applied per value through: spark-pandas_udf

import re
import unicodedata


def clean_html(raw_html):
    cleanr = re.compile(r'<\w+.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

# oxjob #880: digits are identity (`Folder 1983: Scan 29`, `Graduate Catalog 2009-2011`,
# `HII 2927`) -- stripping them collapsed thousands of distinct records onto one work.
def remove_everything_but_alphanumerics(input_string):
    if input_string:
        return "".join(e for e in input_string if e.isalpha() or e.isdigit())
    return ""

def remove_accents(text):
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')


def normalize_title(title):
    if not title:
        return ""

    if isinstance(title, bytes):
        title = str(title, 'ascii')

    text = title[0:500]

    text = text.lower()

    # handle unicode characters
    text = remove_accents(text)

    # remove HTML tags
    text = clean_html(text)

    # remove articles and common prepositions
    text = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from|\n)\b", "", text)

    # remove everything except letters and digits
    text = remove_everything_but_alphanumerics(text)

    return text.strip()
