# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[\"https://www.apache.org/dist/spark/\", \"http://spark.apache.org/docs/latest/\"]"], ["[\"https://github.com/apache/spark/pull/12345\"]"], ["[\"http://www.example.com/path?q=1\", \"https://issues.apache.org/jira/browse/SPARK-1234\", \"https://lists.apache.org/thread.html/abc\"]"], ["[]"], ["[\"https://www.mail-archive.com/user@spark.apache.org/msg123.html\"]"]]
# real-udf: origin=github-spark id=kubeflow_extract_domains requires= wrapped=1 ported=0
# Wrapper: the UDF takes and returns an array of strings; case_entry reads the links from a JSON
# Wrapper: array and returns the result as JSON text. The case module must not import re at top level:
# Wrapper: upstream never imports it, so every element is None (NameError swallowed by the bare except).
# Source: https://github.com/intro-to-ml-with-kubeflow/intro-to-ml-with-kubeflow-examples/blob/b00b44a88011a5397a0287b9197ae211033f454b/feature-prep/spark/SparkMailingListFeaturePrep.py#L140-L153
# Repository: intro-to-ml-with-kubeflow/intro-to-ml-with-kubeflow-examples @ commit b00b44a88011a5397a0287b9197ae211033f454b
# License: Apache-2.0
# Copied verbatim from feature-prep/spark/SparkMailingListFeaturePrep.py (L140-L153); only the module's own import lines that the code uses are prepended.

def extract_domains(links):
    from urllib.parse import urlparse

    def extract_domain(link):
        try:
            nloc = urlparse(link).netloc
            # We want to drop www and any extra spaces wtf nloc on the spaces.
            regex_str = r'^(www\.|)(.*?)\s*$'
            match = re.search(regex_str, nloc)
            return match.group(2)
        except:
            return None

    return list(map(extract_domain, links))


import json


def case_entry(links_json):
    return json.dumps(extract_domains(json.loads(links_json)))
