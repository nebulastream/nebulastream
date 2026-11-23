# codon-fuzz: entry=diff args=VARSIZED,VARSIZED,BOOLEAN returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["host=db1\nport=5432\nuser=app\n", "host=db2\nport=5432\nuser=app\n", false], ["SELECT id, name\nFROM users\nWHERE active = true;\n", "SELECT id, name, email\nFROM users\nWHERE active = true;\n", false], ["line one\nline two\n", "line one\nline two\n", false], ["The quick brown fox\njumps over\nthe lazy dog", "The quick brown cat\njumps over\nthe lazy dog\nand runs", false], ["", "new file content\n", false], ["version: 1.4.2\nrelease: stable\n", "version: 1.5.0\nrelease: beta\n", false]]
# real-udf: origin=databases id=plpython_awesome_diff requires= wrapped=0 ported=0
# Source: https://github.com/vladimir-poghosyan/awesome-plpython/blob/59bdc7f8620c6523a75d997439e4d6a8200fcbfe/scripts/diff.sql#L15
# Commit: 59bdc7f8620c6523a75d997439e4d6a8200fcbfe
# License: MIT
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.

def diff(a, b, as_html):
    from difflib import ndiff, HtmlDiff
    from textwrap import dedent


    # return NULL if any input is NULL
    if a is None or b is None:
        return None

    # split text into lines while preserving line endings
    # this ensures accurate and readable diff output
    a_lines = a.splitlines(keepends=True)
    b_lines = b.splitlines(keepends=True)

    if as_html:
        result = dedent(HtmlDiff().make_table(a_lines, b_lines))
    else:
        result = '\n'.join(ndiff(a_lines, b_lines))

    return result
