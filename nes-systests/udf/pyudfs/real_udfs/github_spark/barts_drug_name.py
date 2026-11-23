# codon-fuzz: entry=extract_drug_name_med_admin args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Paracetamol 500mg tablets"], ["AMOXICILLIN 250 MG CAPSULE"], ["Morphine sulfate 10mg/5ml oral solution"], ["  Salbutamol inhaler  "], ["Ibuprofen"], ["Insulin glargine 100 units/mL"], ["Enoxaparin sodium injection"]]
# real-udf: origin=github-spark id=barts_drug_name requires= wrapped=0 ported=0
# Source: https://github.com/Barts-Life-Science/ADC-DB/blob/c4460f718d082296e5cdcd6ccceea18a9e70c638/Dev/Map%20Pipeline.py#L3287-L3294
# Repository: Barts-Life-Science/ADC-DB @ commit c4460f718d082296e5cdcd6ccceea18a9e70c638
# License: MIT
# Copied verbatim from Dev/Map Pipeline.py (L3287-L3294); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

def extract_drug_name_med_admin(x):
    """Extracts drug name before first number or returns lowercase string"""
    if not x:
        return None
    if any(c.isdigit() for c in x):
        return x.lower().split()[0]
    return x.lower().strip()
