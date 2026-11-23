# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED,VARSIZED,INT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["$685,000", "sale", "3 bds , 2 ba , 1,560 sqft", 1560], ["$1,250,000", "sale", "5 bds , 4 ba , 3,950 sqft", 3950], ["$2,450/mo", "rent", "2 bds , 1 ba , 850 sqft", 850], ["$3,100/mo", "rent", "3 bds , 2 ba , 1,200 sqft", 1200], ["$382,200", "sold", "3 bds , 2 ba , 1,560 sqft , Price/sqft: $245 , Built 1925", 1560], ["$529,000", "foreclosed", "4 bds , 3 ba , 2,000 sqft", 2000]]
# real-udf: origin=literature id=tuplex_zillow_extract_price requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/zillow/Z1/runtuplex.py#L81-L98
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/zillow/Z1/runtuplex.py, lines 81-98

def extractPrice(x):
    price = x['price']
    p = 0
    if x['offer'] == 'sold':
        # price is to be calculated using price/sqft * sqft
        val = x['facts and features']
        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]
        r = s[s.find('$')+1:s.find(', ') - 1]
        price_per_sqft = int(r)
        p = price_per_sqft * x['sqft']
    elif x['offer'] == 'rent':
        max_idx = price.rfind('/')
        p = int(price[1:max_idx].replace(',', ''))
    else:
        # take price from price column
        p = int(price[1:].replace(',', ''))

    return p


def case_entry(price, offer, facts_and_features, sqft):
    return extractPrice({"price": price, "offer": offer, "facts and features": facts_and_features, "sqft": sqft})
