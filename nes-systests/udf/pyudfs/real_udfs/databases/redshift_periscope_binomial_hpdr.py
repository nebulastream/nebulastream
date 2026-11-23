# codon-fuzz: entry=binomial_hpdr args=FLOAT64,INT64,INT64,FLOAT64,FLOAT64,FLOAT64,INT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0.12, 120, 1000, 0.95, 1.0, 1.0, 1000], [0.3, 120, 1000, 0.95, 1.0, 1.0, 1000], [0.05, 120, 1000, 0.95, 1.0, 1.0, 1000], [0.7, 700, 1000, 0.9, 1.0, 1.0, 500], [0.5, 48, 100, 0.95, 2.0, 2.0, 1000]]
# real-udf: origin=databases id=redshift_periscope_binomial_hpdr requires=numpy,scipy wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_stats.rb#L29
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def binomial_hpdr(value, successes, samples, pct, a, b, n_pbins):
    # Taken from:
    # http://stackoverflow.com/a/19285227
    import numpy
    from scipy.stats import beta
    from scipy.stats import norm
    n = successes
    N = samples
    rv = beta(n+a, N-n+b)
    stdev = rv.stats('v')**0.5
    mode = (n+a-1.)/(N+a+b-2.)
    n_sigma = numpy.ceil(norm.ppf( (1+pct)/2. ))+1
    max_p = mode + n_sigma * stdev
    if max_p > 1:
      max_p = 1.
    min_p = mode - n_sigma * stdev
    if min_p > 1:
      min_p = 1.
    p_range = numpy.linspace(min_p, max_p, n_pbins+1)
    if mode > 0.5:
        sf = rv.sf(p_range)
        pmf = sf[:-1] - sf[1:]
    else:
        cdf = rv.cdf(p_range)
        pmf = cdf[1:] - cdf[:-1]
    # find the upper and lower bounds of the interval 
    sorted_idxs = numpy.argsort( pmf )[::-1]
    cumsum = numpy.cumsum( numpy.sort(pmf)[::-1] )
    j = numpy.argmin( numpy.abs(cumsum - pct) )
    upper = p_range[ (sorted_idxs[:j+1]).max()+1 ]
    lower = p_range[ (sorted_idxs[:j+1]).min() ]
    if (value >= lower and value <= upper):
      return 0
    elif value >= upper:
      return 1
    else:
      return -1
