# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["astrazeneca"], ["coronavac"], ["pfizer"], ["janssen"], ["covid-19 astrazeneca/fiocruz"], ["coronavac sinovac/butantan"]]
# real-udf: origin=github-spark id=pamepi_rename_vaccine requires= wrapped=1 ported=0
# Wrapper: the UDF returns an int code or the input text and is registered with udf() (StringType);
# Wrapper: case_entry returns str() of the result.
# Source: https://github.com/PAMepi/PAMepi_scripts_datalake/blob/404673c8c0e7b45f0f5ddcba963310f3a15979db/Brazilian%20COVID-19%20data%20streaming/Data%20curation/myfunctions.py#L30-L41
# Repository: PAMepi/PAMepi_scripts_datalake @ commit 404673c8c0e7b45f0f5ddcba963310f3a15979db
# License: MIT
# Copied verbatim from Brazilian COVID-19 data streaming/Data curation/myfunctions.py (L30-L41); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf()

def renomeia_vacina(col):
    if col.find('astrazeneca'):
        return 1
    elif col.find('coronavac'):
        return 2
    elif col.find('pfizer'):
        return 3
    elif col.find('janssen'):
        return 4
    else:
        return col


def case_entry(col):
    return str(renomeia_vacina(col))
