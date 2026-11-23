# codon-fuzz: entry=fn_levenshtein_distance args=VARSIZED,VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["kitten", "sitting"], ["flaw", "lawn"], ["", "abc"], ["Main Street", "Main St"], ["identical", "identical"], ["Schwarzenegger", "Schwarzeneger"], ["abc", ""]]
# real-udf: origin=databases id=redshift_aws_fn_levenshtein_distance requires= wrapped=0 ported=0
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/fn_levenshtein_distance%28varchar%2Cvarchar%29/function.sql#L1
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def fn_levenshtein_distance(a, b):
    def levenshtein_distance(a, len_a, b, len_b):
        d = [[0] * (len_b + 1) for i in range(len_a + 1)]  

        for i in range(1, len_a + 1):
            d[i][0] = i

        for j in range(1, len_b + 1):
            d[0][j] = j

        for j in range(1, len_b + 1):
            for i in range(1, len_a + 1):
                if a[i - 1] == b[j - 1]:
                    cost = 0
                else:
                    cost = 1
                d[i][j] = min(d[i - 1][j] + 1,      # deletion
                              d[i][j - 1] + 1,      # insertion
                              d[i - 1][j - 1] + cost) # substitution   

        return d[len_a][len_b]

    def distance(a, b):
    	if a is None:
    		len_a = 0
    	else:
    		len_a = len(a)
    	if b is None:
    		len_b = 0
    	else:
    		len_b = len(b)
    	if len_a == 0:
    		return len_b
    	elif len_b == 0:
    		return len_a
    	else:
    		return levenshtein_distance(a, len_a, b, len_b)

    return distance(a, b)
