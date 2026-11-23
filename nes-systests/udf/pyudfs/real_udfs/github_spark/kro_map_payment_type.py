# codon-fuzz: entry=map_payment_type args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[1], [2], [3], [4], [5], [6], [0], [7]]
# real-udf: origin=github-spark id=kro_map_payment_type requires= wrapped=0 ported=0
# Source: https://github.com/kubernetes-sigs/kro/blob/1cfd2793e3520b7944af61257fbffe71b19228e8/examples/aws/data-processor/scripts/summarize_nyc_taxi_data.py#L11-L20
# Repository: kubernetes-sigs/kro @ commit 1cfd2793e3520b7944af61257fbffe71b19228e8
# License: Apache-2.0
# Copied verbatim from examples/aws/data-processor/scripts/summarize_nyc_taxi_data.py (L11-L20); only the module's own import lines that the code uses are prepended.

def map_payment_type(payment_type):
    payment_map = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
    return payment_map.get(payment_type, "Other")
