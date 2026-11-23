# codon-fuzz: entry=standardize args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["12 St. Mary's Rd., London"], ["Flat 3, 45 Victoria Ave"], ["1st Floor & Bsmt, 7 Kings Cres"], ["Bldg 4 (rear), Royal London Hse"], ["22 \"The Gdns\" Ter*"], ["Gnd Flt, 9 Whitechapel Sq."], ["  3rd floor, 80 Newark St  "]]
# real-udf: origin=github-spark id=barts_standardize_address requires= wrapped=0 ported=0
# Source: https://github.com/Barts-Life-Science/ADC-DB/blob/c4460f718d082296e5cdcd6ccceea18a9e70c638/Dev/Map%20Pipeline.py#L476-L517
# Repository: Barts-Life-Science/ADC-DB @ commit c4460f718d082296e5cdcd6ccceea18a9e70c638
# License: MIT
# Copied verbatim from Dev/Map Pipeline.py (L476-L517); only the module's own import lines that the code uses are prepended.

def standardize(address):
    if not address:
        return ""

    # Convert to uppercase and basic cleaning
    address = address.upper().strip()

    # Remove special characters (ASSIGN approach)
    address = address.replace(",", " ")
    address = address.replace("'", "")
    address = address.replace('"', "")
    address = address.replace("%", "")
    address = address.replace("*", " ")
    address = address.replace(".", " ")
    address = address.replace("(", " ")
    address = address.replace(")", " ")

    # Normalize ampersands
    address = address.replace(" & ", " AND ")

    # Remove multiple spaces
    address = " ".join(address.split())

    # Common corrections (subset of ASSIGN's extensive dictionary)
    corrections = {
        "ST": "STREET", "RD": "ROAD", "AVE": "AVENUE",
        "GDNS": "GARDENS", "CRES": "CRESCENT", "TER": "TERRACE",
        "SQ": "SQUARE", "CT": "COURT", "PL": "PLACE",
        "BLDG": "BUILDING", "HSE": "HOUSE", "FLT": "FLAT",
        "1ST": "FIRST", "2ND": "SECOND", "3RD": "THIRD",
        "GND": "GROUND", "BSMT": "BASEMENT"
    }

    words = address.split()
    corrected_words = []

    for word in words:
        # Apply corrections if found
        corrected_word = corrections.get(word, word)
        corrected_words.append(corrected_word)

    return " ".join(corrected_words)
