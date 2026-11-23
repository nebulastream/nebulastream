# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1600 Pennsylvania Ave NW, Washington, DC 20500"], ["123 Main St Apt 4B, Springfield, IL 62704"], ["350 5th Avenue, New York, NY 10118"], ["1 Infinite Loop Cupertino CA 95014"], ["PO Box 1234, Austin, TX 78701"], ["742 Evergreen Terrace, Springfield"]]
# real-udf: origin=databases id=snowflake_sfsamples_parse_address requires=usaddress wrapped=1 ported=0
# Wrapper: The UDF returns a dict (Snowflake OBJECT); case_entry returns it as json.dumps(..., sort_keys=True).
# Source: https://github.com/Snowflake-Labs/sf-samples/blob/96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f/samples/geospatial/geocoding/sql/udfs/parse_address.sql#L15
# Commit: 96ef1b2c7b44ad6cdd8fc18c7d1be426e569196f
# License: Apache-2.0
# Python body of the CREATE FUNCTION ... LANGUAGE PYTHON statement (handler-based, copied as is).

import usaddress

def parse(raw_address):
    if not raw_address:
        return None
    try:
        tagged, addr_type = usaddress.tag(raw_address)
    except usaddress.RepeatedLabelError:
        return {
            "number": None,
            "street": raw_address,
            "city": None,
            "state": None,
            "zip": None,
            "parse_error": True
        }

    street_parts = []
    for key in ['StreetNamePreDirectional', 'StreetName', 'StreetNamePostType', 'StreetNamePostDirectional']:
        if key in tagged:
            street_parts.append(tagged[key])

    return {
        "number": tagged.get('AddressNumber'),
        "street": ' '.join(street_parts) if street_parts else None,
        "city": tagged.get('PlaceName'),
        "state": tagged.get('StateName'),
        "zip": tagged.get('ZipCode'),
        "parse_error": False
    }


def case_entry(raw_address):
    import json
    return json.dumps(parse(raw_address), sort_keys=True)
