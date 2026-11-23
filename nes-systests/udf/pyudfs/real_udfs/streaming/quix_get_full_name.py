# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Jane", "Doe"], ["Hans-Peter", "Müller"], ["María José", "García López"], ["", "Prince"], ["Li", "Wei"]]
# real-udf: origin=streaming id=quix_get_full_name requires= wrapped=1 ported=0
# Wrapper: case_entry builds the customer dict from the two name fields.

# Source: https://github.com/quixio/quix-streams/blob/e7fd055b09a0a6d458213be69c40064869ba3e82/docs/tutorials/purchase-filtering/tutorial_app.py#L78-L79
# Repository: https://github.com/quixio/quix-streams (commit e7fd055b09a0a6d458213be69c40064869ba3e82)
# License: Apache-2.0
# System: Quix Streams
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def get_full_name(customer):
    return f'{customer["First Name"]} {customer["Last Name"]}'


def case_entry(first_name, last_name):
    return get_full_name({"First Name": first_name, "Last Name": last_name})
