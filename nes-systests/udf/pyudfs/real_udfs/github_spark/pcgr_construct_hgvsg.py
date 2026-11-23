# codon-fuzz: entry=construct_hgvsg args=VARSIZED,INT64,INT64,VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["7", 140453136, 140453136, "A", "T"], ["17", 7579472, 7579474, "GCA", "-"], ["13", 32914437, 32914437, "GT", "G"], ["X", 153296777, 153296778, "-", "AG"], ["1", 11188164, 11188165, "CT", "GA"], ["MT", 3243, 3243, "A", "G"], ["12", 25398284, 25398284, "C", "CTT"]]
# real-udf: origin=github-spark id=pcgr_construct_hgvsg requires=pandas wrapped=0 ported=0
# Source: https://github.com/sigven/pcgr/blob/06ead78273666727bbdedafb00bcb7d804e00f8a/pcgr/maf.py#L194-L266
# Repository: sigven/pcgr @ commit 06ead78273666727bbdedafb00bcb7d804e00f8a
# License: MIT
# Copied verbatim from pcgr/maf.py (L194-L266); only the module's own import lines that the code uses are prepended.
# Applied per value through: pandas-apply

import pandas as pd


def construct_hgvsg(chr_val, start, end, ref, alt):
    """
    Construct HGVSg notation from variant coordinates and alleles.

    Args:
        chr_val: Chromosome (e.g., "1", "X", "MT")
        start: Start position (1-based)
        end: End position (1-based)
        ref: Reference allele
        alt: Alternate allele

    Returns:
        HGVSg notation string (e.g., "1:g.12345A>T") or empty string if invalid
    """
    # Return empty string if any required field is missing or NA
    if pd.isna(chr_val) or pd.isna(start) or pd.isna(end) or pd.isna(ref) or pd.isna(alt):
        return ""

    # Convert to strings to handle properly
    chr_val = str(chr_val)
    start_str = str(start)
    end_str = str(end)
    ref = str(ref)
    alt = str(alt)

    # Handle NaN strings
    if chr_val == 'nan' or start_str == 'nan' or end_str == 'nan' or ref == 'nan' or alt == 'nan':
        return ""

    # Handle empty strings
    if not chr_val or not start_str or not end_str or not ref or not alt:
        return ""

    # SNV: single position, reference != alt, both single base
    if start_str == end_str and len(ref) == 1 and len(alt) == 1:
        return f"{chr_val}:g.{start_str}{ref}>{alt}"

    # Deletion: reference longer than alt
    if len(ref) > len(alt):
        if len(alt) == 0 or alt == "-":
            # Pure deletion
            if start_str == end_str:
                return f"{chr_val}:g.{start_str}del"
            else:
                return f"{chr_val}:g.{start_str}_{end_str}del"
        else:
            # Deletion with replacement (delins)
            if start_str == end_str:
                return f"{chr_val}:g.{start_str}delins{alt}"
            else:
                return f"{chr_val}:g.{start_str}_{end_str}delins{alt}"

    # Insertion: alt longer than reference
    if len(alt) > len(ref):
        if len(ref) == 0 or ref == "-":
            # Pure insertion
            return f"{chr_val}:g.{start_str}_{end_str}ins{alt}"
        else:
            # Complex indel (delins)
            if start_str == end_str:
                return f"{chr_val}:g.{start_str}delins{alt}"
            else:
                return f"{chr_val}:g.{start_str}_{end_str}delins{alt}"

    # MNV or delins: same length but different
    if len(ref) == len(alt) and len(ref) > 1:
        if start_str == end_str:
            return f"{chr_val}:g.{start_str}delins{alt}"
        else:
            return f"{chr_val}:g.{start_str}_{end_str}delins{alt}"

    # Default case
    return ""
