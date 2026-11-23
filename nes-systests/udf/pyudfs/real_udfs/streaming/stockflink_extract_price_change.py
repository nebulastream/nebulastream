# codon-fuzz: entry=extract_price_change args=VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["+2.35%"], ["-0.87%"], [" 0.00% "], ["12.5"], ["N/A"], ["--"], ["-15.2 %"]]
# real-udf: origin=streaming id=stockflink_extract_price_change requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.

# Source: https://github.com/shosseini811/Stock-Apache-Flink/blob/00a2d872a7d78e26a710754786c746539d286bfd/src/processing/flink_processor.py#L79-L84
# Repository: https://github.com/shosseini811/Stock-Apache-Flink (commit 00a2d872a7d78e26a710754786c746539d286bfd)
# License: MIT
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def extract_price_change(change_str):
    try:
        return float(change_str.replace('%', '').strip())
    except:
        return 0.0
