# codon-fuzz: entry=contains_probably_java_stack_trace args=VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["I get this error:\njava.lang.NullPointerException: null\n\tat org.apache.spark.rdd.RDD.count(RDD.scala:1213)\n\tat Main.main(Main.java:10)\n"], ["Hi all, how do I set spark.executor.memory in spark-submit?"], ["Exception in thread \"main\" org.apache.spark.SparkException: Job aborted:\nCaused by: stage failure\n    at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:1889)\n"], ["The IOException went away after upgrading."], ["Traceback (most recent call last):\n  File \"job.py\", line 3, in <module>\nValueError: bad value\n"]]
# real-udf: origin=github-spark id=kubeflow_java_stack_trace requires= wrapped=0 ported=0
# Source: https://github.com/intro-to-ml-with-kubeflow/intro-to-ml-with-kubeflow-examples/blob/b00b44a88011a5397a0287b9197ae211033f454b/feature-prep/spark/SparkMailingListFeaturePrep.py#L160-L168
# Repository: intro-to-ml-with-kubeflow/intro-to-ml-with-kubeflow-examples @ commit b00b44a88011a5397a0287b9197ae211033f454b
# License: Apache-2.0
# Copied verbatim from feature-prep/spark/SparkMailingListFeaturePrep.py (L160-L168); only the module's own import lines that the code uses are prepended.

def contains_probably_java_stack_trace(body):
    # Look for something based on regex
    # Tried https://stackoverflow.com/questions/20609134/regular-expression-optional-multiline-java-stacktrace - more msg looking
    # Tried https://stackoverflow.com/questions/3814327/regular-expression-to-parse-a-log-file-and-find-stacktraces
    # Yes the compile is per call, but it's cached so w/e
    import re
    stack_regex_str = r'^\s*(.+Exception.*):\n(.*\n){0,3}?(\s+at\s+.*\(.*\))+'
    match = re.search(stack_regex_str, body, re.MULTILINE)
    return match is not None
