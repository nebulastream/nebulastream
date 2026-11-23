# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["To be, or not to be,--that is the question:--"], ["Whether 'tis nobler in the mind to suffer"], ["   "], ["The slings and arrows of outrageous fortune\n"], ["Or to take arms\tagainst a sea of troubles,"], [""]]
# real-udf: origin=streaming id=pyflink_ds_split_words requires= wrapped=1 ported=0
# Wrapper: split is a flat_map generator; case_entry joins the words it yields with "|".

# Source: https://github.com/apache/flink/blob/b1f051d09c4111898902167428199c00747a5076/flink-python/pyflink/examples/datastream/word_count.py#L85-L86
# Repository: https://github.com/apache/flink (commit b1f051d09c4111898902167428199c00747a5076)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def split(line):
    yield from line.split()


def case_entry(line):
    return "|".join(split(line))
