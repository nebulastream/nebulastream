# codon-fuzz: entry=case_entry args=FLOAT64,INT64,FLOAT64,INT64,FLOAT64,INT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[19.99, 2, 5.49, 1, 120.0, 1], [3.5, 10, 0.99, 3, 12.25, 0], [49.95, 1, 49.95, 1, 49.95, 1], [0.1, 3, 0.2, 3, 0.3, 3], [1299.0, 1, 24.99, 2, 9.99, 4]]
# real-udf: origin=streaming id=quix_get_purchase_totals requires= wrapped=1 ported=0
# Wrapper: get_purchase_totals takes a list of item dicts; case_entry builds it from three (price, quantity) items.

# Source: https://github.com/quixio/quix-streams/blob/e7fd055b09a0a6d458213be69c40064869ba3e82/docs/tutorials/purchase-filtering/tutorial_app.py#L82-L83
# Repository: https://github.com/quixio/quix-streams (commit e7fd055b09a0a6d458213be69c40064869ba3e82)
# License: Apache-2.0
# System: Quix Streams
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

SALES_TAX = 1.10


def get_purchase_totals(items):
    return sum([i["Price"] * i["Quantity"] for i in items])


def case_entry(price1, quantity1, price2, quantity2, price3, quantity3):
    return get_purchase_totals([
        {"Price": price1, "Quantity": quantity1},
        {"Price": price2, "Quantity": quantity2},
        {"Price": price3, "Quantity": quantity3},
    ])
