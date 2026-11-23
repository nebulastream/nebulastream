# codon-fuzz: entry=case_entry args=FLOAT64,FLOAT64,FLOAT64,FLOAT64,FLOAT64,FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[67250.5, 0.42, 67251.0, 1.15, 67249.75, 0.08], [3120.12, 12.0, 3119.8, 4.5, 3121.0, 7.25], [0.6123, 15000.0, 0.6119, 22000.0, 0.6125, 9000.0], [189.45, 100.0, 189.5, 250.0, 189.4, 50.0], [67300.0, 0.0, 67310.0, 2.0, 67290.0, 0.5], [145.1, 3.0, 144.95, 1.0, 145.2, 6.0]]
# real-udf: origin=streaming id=jkaiml_calculate_vwap requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.
# Wrapper: calculate_vwap takes a list of prices and a list of volumes; case_entry builds both lists from three (price, volume) trades.
# Imports: the original module's imports and its module-level logger.

import logging

logger = logging.getLogger(__name__)

# Source: https://github.com/JK-AIML/High-Frequency-Cryptocurrency-Trading-Platform/blob/326809f57f52e1412c9c839240a333cd4796b06c/src/backend/tick_analysis/streaming/flink_processor.py#L142-L153
# Repository: https://github.com/JK-AIML/High-Frequency-Cryptocurrency-Trading-Platform (commit 326809f57f52e1412c9c839240a333cd4796b06c)
# License: MIT
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def calculate_vwap(prices, volumes):
    if not prices or not volumes or len(prices) != len(volumes):
        return None
    try:
        total_volume = sum(volumes)
        if total_volume <= 0:
            return None
        return sum(p * v for p, v in zip(prices, volumes)) / total_volume
    except Exception as e:
        logger.error(f"Error in VWAP calculation: {e}")
        return None


def case_entry(price1, volume1, price2, volume2, price3, volume3):
    return calculate_vwap([price1, price2, price3], [volume1, volume2, volume3])
