# codon-fuzz: entry=get_water_heater_capacity_ashrae args=INT64,FLOAT64,BOOLEAN returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[1, 1.0, false], [2, 1.5, true], [2, 2.5, false], [2, 3.0, true], [3, 1.0, true], [3, 2.0, false], [4, 2.5, true], [4, 3.5, false], [5, 3.0, true], [6, 4.0, false]]
# real-udf: origin=github-spark id=rewiring_water_heater_capacity requires= wrapped=0 ported=0
# Source: https://github.com/rewiringamerica/surrogate_modeling/blob/8e5900d9f9f4f4ec695572560f1ef7ff0811d80a/src/feature_utils.py#L251-L313
# Repository: rewiringamerica/surrogate_modeling @ commit 8e5900d9f9f4f4ec695572560f1ef7ff0811d80a
# License: Apache-2.0
# Copied verbatim from src/feature_utils.py (L251-L313); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(IntegerType())

def get_water_heater_capacity_ashrae(n_bedrooms: int, n_bathrooms: float, is_electric: bool) -> int:
    """
    Calculate the recommended water heater capacity in gallons based on the number of bedrooms, bathrooms, and whether the heater is electric.

    Source: https://www.nrel.gov/docs/fy10osti/47246.pdf
    Table 8. Benchmark Domestic Hot Water Storage and Burner Capacity (ASHRAE 1999)

    >>> get_water_heater_capacity_ashrae(3, 4, False)
    40
    >>> get_water_heater_capacity_ashrae(1, 1.5, True)
    20
    >> get_water_heater_capacity_ashrae(6, 5, True)
    80
    """
    match n_bedrooms:
        case 1:
            return 20
        case 2:
            if n_bathrooms < 2:
                return 30
            elif n_bathrooms < 3:
                if is_electric:
                    return 40
                else:
                    return 30
            else:
                if is_electric:
                    return 50
                else:
                    return 40
        case 3:
            if n_bathrooms < 2:
                if is_electric:
                    return 40
                else:
                    return 30
            else:
                if is_electric:
                    return 50
                else:
                    return 40
        case 4:
            if n_bathrooms < 3:
                if is_electric:
                    return 50
                else:
                    return 40
            else:
                if is_electric:
                    return 66
                else:
                    return 50
        case 5:
            if is_electric:
                return 66
            else:
                return 50
        case 6:
            if is_electric:
                return 80
            else:
                return 50
