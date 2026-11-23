# codon-fuzz: entry=experiment_result_p_value args=INT64,INT64,INT64,INT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[10000, 520, 10000, 610], [5000, 250, 5100, 255], [1200, 96, 1180, 132], [250000, 7500, 249000, 7900], [800, 40, 820, 39]]
# real-udf: origin=databases id=redshift_periscope_experiment_result_p_value requires=numpy,scipy wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_stats.rb#L5
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def experiment_result_p_value(control_size, control_conversion, experiment_size, experiment_conversion):
    from scipy.stats import chi2_contingency
    from numpy import array
    observed = array([
      [control_size - control_conversion, control_conversion],
      [experiment_size - experiment_conversion, experiment_conversion]
    ])
    result = chi2_contingency(observed, correction=True)
    chisq, p = result[:2]
    return p
