# codon-fuzz: entry=haversine_sql args=FLOAT64,FLOAT64,FLOAT64,FLOAT64,VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[52.52, 13.405, 48.8566, 2.3522, "km"], [40.7128, -74.006, 34.0522, -118.2437, "mi"], [51.5072, -0.1276, 51.5072, -0.1276, "km"], [-33.8688, 151.2093, 35.6762, 139.6503, "nmi"], [47.3769, 8.5417, 47.3686, 8.5392, "m"]]
# real-udf: origin=databases id=sqlite_datasette_haversine requires=haversine wrapped=0 ported=0
# Source: https://github.com/simonw/datasette-haversine/blob/3b045775c30fd191f92cf358988580b948d9f081/datasette_haversine/__init__.py#L5
# Commit: 3b045775c30fd191f92cf358988580b948d9f081
# License: Apache-2.0

from haversine import haversine


def haversine_sql(lat1, lon1, lat2, lon2, unit="km"):
    return haversine((float(lat1), float(lon1)), (float(lat2), float(lon2)), unit=unit)
