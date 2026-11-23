# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["ramesh.kumar@okhdfcbank"], ["9876543210@paytm"], ["priya.s@ybl"], ["merchant.store123@icici"], ["a@upi"]]
# real-udf: origin=github-spark id=upi_hash_id requires= wrapped=1 ported=0
# Wrapper: hash_upi_id is a method that does not use self; case_entry passes None for self.
# Source: https://github.com/Skismail57/UPI_Fraud_Detection_Using_Machine_Learning/blob/557974ec16f842f7ac09fa450da3dff546c2225b/streaming/spark_streaming.py#L125-L127
# Repository: Skismail57/UPI_Fraud_Detection_Using_Machine_Learning @ commit 557974ec16f842f7ac09fa450da3dff546c2225b
# License: MIT
# Copied verbatim from streaming/spark_streaming.py (L125-L127); only the module's own import lines that the code uses are prepended.

import hashlib


def hash_upi_id(self, upi_id: str) -> str:
    """Hash UPI ID for privacy protection"""
    return hashlib.sha256(upi_id.encode()).hexdigest()[:16]


def case_entry(upi_id):
    return hash_upi_id(None, upi_id)
