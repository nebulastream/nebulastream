# codon-fuzz: entry=f_cosine_similarity args=VARSIZED,VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[1, 2, 3]", "[4, 5, 6]"], ["[0.12, -0.53, 0.88, 0.07]", "[0.10, -0.49, 0.91, 0.02]"], ["[1, 0]", "[0, 1]"], ["[3.5, 2.0, 1.0]", "[-3.5, -2.0, -1.0]"], ["[0.2, 0.4, 0.4, 0.1, 0.9]", "[0.3, 0.1, 0.5, 0.0, 0.8]"]]
# real-udf: origin=databases id=redshift_aws_f_cosine_similarity requires=numpy wrapped=0 ported=0
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_cosine_similarity%28varchar%2Cvarchar%29/function.sql#L7
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_cosine_similarity(v1, v2):
    import numpy,json
    from numpy.linalg import norm
    A = numpy.array(json.loads(v1))
    B = numpy.array(json.loads(v2))
    return numpy.dot(A,B)/(norm(A)*norm(B))
