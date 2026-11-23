# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["John", "A.", "Smith"], ["Maria", "", "Garcia"], ["Li", "Wei", "Chen"], ["Robert", "J.", "O'Neil"], ["Aisha", "", "Khan"], ["", "", "Unknown"]]
# real-udf: origin=github-spark id=insurancelake_format_column requires= wrapped=1 ported=0
# Wrapper: format_column takes a format string and any number of column values; case_entry passes
# Wrapper: the format string '{} {} {}' (a full-name column) and three name columns.
# Source: https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/179c6c7ccb55dc95ea784138a845161a1633f3f2/lib/glue_scripts/lib/datatransform_stringmanipulation.py#L112-L119
# Repository: aws-solutions-library-samples/aws-insurancelake-etl @ commit 179c6c7ccb55dc95ea784138a845161a1633f3f2
# License: MIT-0
# Copied verbatim from lib/glue_scripts/lib/datatransform_stringmanipulation.py (L112-L119); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(StringType())

def format_column(formatstring, *args):
    """Format list of column values using a Python format string; convert None values to
    empty strings and attempt to clean up dangling whitespace separators
    """
    if formatstring:
        checked_args = [ '' if value is None else value for value in args ]
        return formatstring.format(*checked_args).strip()


def case_entry(first_name, middle_name, last_name):
    return format_column("{} {} {}", first_name, middle_name, last_name)
