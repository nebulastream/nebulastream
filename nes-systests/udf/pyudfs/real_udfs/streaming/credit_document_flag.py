# codon-fuzz: entry=document_flag args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["079-085-12345"], [""], ["   "], ["CMND 012345678"], ["P1234567"], ["\t\n"]]
# real-udf: origin=streaming id=credit_document_flag requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.

# Source: https://github.com/ngnquanq/credit-risk-ml-system/blob/ff2442ed39d3f8b377e4aa52e668a494a5bd4b4b/application/flink/jobs/cdc_udfs.py#L117-L128
# Repository: https://github.com/ngnquanq/credit-risk-ml-system (commit ff2442ed39d3f8b377e4aa52e668a494a5bd4b4b)
# License: MIT
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def document_flag(document_id: str) -> int:
    """
    Convert document ID to binary flag (1 if exists and not empty, 0 otherwise).

    Args:
        document_id: Document identifier string

    Returns:
        1 if document exists and is not empty, 0 otherwise
    """
    return 1 if document_id and len(document_id.strip()) > 0 else 0
