# codon-fuzz: entry=case_entry args=VARSIZED,FLOAT64,FLOAT64,FLOAT64,BOOLEAN returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["cpu_usage", 0.83, 0.4215, 0.1132, true], ["memory_mb", 2048.0, 2001.375, 35.5, false], ["latency_ms", 12.5, 11.9, 0.004, true], ["temperature_c", 21.0, 21.0, 0.0, false], ["requests_per_s", 1520.25, 1480.0, 25.125, false]]
# real-udf: origin=streaming id=bytewax_pretty_formatter requires= wrapped=1 ported=0
# Wrapper: case_entry builds the (metric, (value, mu, sigma, is_anomalous)) tuple.

# Source: https://github.com/bytewax/bytewax/blob/2fe2cd3b220195aa614c3effb50f31925dcaa836/examples/anomaly_detector.py#L55-L63
# Repository: https://github.com/bytewax/bytewax (commit 2fe2cd3b220195aa614c3effb50f31925dcaa836)
# License: Apache-2.0
# System: Bytewax
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def pretty_formatter(key_value):
    metric, (value, mu, sigma, is_anomalous) = key_value
    return (
        f"{metric}: "
        f"value = {value}, "
        f"mu = {mu:.2f}, "
        f"sigma = {sigma:.2f}, "
        f"{is_anomalous}"
    )


def case_entry(metric, value, mu, sigma, is_anomalous):
    return pretty_formatter((metric, (value, mu, sigma, is_anomalous)))
