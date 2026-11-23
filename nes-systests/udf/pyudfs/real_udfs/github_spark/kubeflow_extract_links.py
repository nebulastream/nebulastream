# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["See https://spark.apache.org/docs/latest/ for details.\nAlso http://example.com/a?b=c"], ["No links in this message, just a question about shuffle partitions."], ["PR: https://github.com/apache/spark/pull/12345\nJIRA: https://issues.apache.org/jira/browse/SPARK-1234"], ["Mirror at http://www.apache.org/dyn/closer.lua/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz"], ["Thanks!\n\nOn Tue, Jan 7, 2020 at 10:15 AM <user@spark.apache.org> wrote:\n> see https://s.apache.org/abc"]]
# real-udf: origin=github-spark id=kubeflow_extract_links requires= wrapped=1 ported=0
# Wrapper: the UDF returns an array of strings; case_entry returns it as JSON text.
# Source: https://github.com/intro-to-ml-with-kubeflow/intro-to-ml-with-kubeflow-examples/blob/b00b44a88011a5397a0287b9197ae211033f454b/feature-prep/spark/SparkMailingListFeaturePrep.py#L133-L137
# Repository: intro-to-ml-with-kubeflow/intro-to-ml-with-kubeflow-examples @ commit b00b44a88011a5397a0287b9197ae211033f454b
# License: Apache-2.0
# Copied verbatim from feature-prep/spark/SparkMailingListFeaturePrep.py (L133-L137); only the module's own import lines that the code uses are prepended.

def extract_links(body):
    import re
    link_regex_str = r'(http(|s)://(.*?))([\s\n]|$)'
    itr = re.finditer(link_regex_str, body, re.MULTILINE)
    return list(map(lambda elem: elem.group(1), itr))


import json


def case_entry(body):
    return json.dumps(extract_links(body))
