# codon-fuzz: entry=_to_e164 args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["(415) 555-0132", "US"], ["020 7946 0958", "GB"], ["030 901820", "DE"], ["+33 1 42 68 53 00", "US"], ["098765 43210", "IN"], ["0412 345 678", "AU"]]
# real-udf: origin=github-spark id=spindle_phone_to_e164 requires=phonenumbers wrapped=0 ported=0
# Source: https://github.com/spindle-health/spindle-token/blob/dfd5b33294a6ad5f8f41ec5a6a4a1ddc7a8d7c06/src/spindle_token/opprl/_common.py#L199-L206
# Repository: spindle-health/spindle-token @ commit dfd5b33294a6ad5f8f41ec5a6a4a1ddc7a8d7c06
# License: Apache-2.0
# Copied verbatim from src/spindle_token/opprl/_common.py (L199-L206); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

import phonenumbers


def _to_e164(phone: str, default_region: str) -> str | None:
    try:
        return phonenumbers.format_number(
            phonenumbers.parse(phone, default_region), phonenumbers.PhoneNumberFormat.E164
        )
    except phonenumbers.NumberParseException:
        return None
