# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["25A High Street", ""], ["12 Whitechapel Road", "Flat 4B"], ["Flat 7, Victoria House", "Commercial Street"], ["Apartment 12 Canary Wharf", ""], ["101 Mile End Road", "Unit 5 Queen Mary Business Centre"], ["The Old Rectory", "Church Lane"], ["3 Bow Common Lane", "Apt 2"]]
# real-udf: origin=github-spark id=barts_extract_building_components requires= wrapped=1 ported=0
# Wrapper: the UDF returns a struct (dict); case_entry returns it as JSON text.
# Source: https://github.com/Barts-Life-Science/ADC-DB/blob/c4460f718d082296e5cdcd6ccceea18a9e70c638/Dev/Map%20Pipeline.py#L523-L563
# Repository: Barts-Life-Science/ADC-DB @ commit c4460f718d082296e5cdcd6ccceea18a9e70c638
# License: MIT
# Copied verbatim from Dev/Map Pipeline.py (L523-L563); only the module's own import lines that the code uses are prepended.

def extract(street_addr, street_addr2):
    result = {
        "number": "",
        "flat": "",
        "building_name": ""
    }

    if not street_addr:
        return result

    # Extract number from start of address
    import re
    number_match = re.match(r'^(\d+[A-Z]?)\s', street_addr.upper())
    if number_match:
        result["number"] = number_match.group(1)

        # Check for flat suffix (e.g., "25A" where A is flat)
        if re.match(r'^\d+[A-Z]$', result["number"]):
            result["flat"] = result["number"][-1]
            result["number"] = result["number"][:-1]

    # Extract flat/apartment indicators
    flat_patterns = [
        r'FLAT\s+(\w+)',
        r'APARTMENT\s+(\w+)',
        r'APT\s+(\w+)',
        r'UNIT\s+(\w+)'
    ]

    combined_addr = f"{street_addr} {street_addr2 or ''}".upper()
    for pattern in flat_patterns:
        flat_match = re.search(pattern, combined_addr)
        if flat_match and not result["flat"]:
            result["flat"] = flat_match.group(1)
            break

    # Building name is typically in addr2 or after number in addr1
    if street_addr2:
        result["building_name"] = street_addr2.upper()

    return result


import json


def case_entry(street_addr, street_addr2):
    return json.dumps(extract(street_addr, street_addr2), sort_keys=True)
