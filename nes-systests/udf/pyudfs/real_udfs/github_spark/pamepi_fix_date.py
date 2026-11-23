# codon-fuzz: entry=corrige_data args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["25/03/2021"], ["2021/03/25"], ["2021-03-25"], ["01/12/2020"], ["ignorado"], ["31/01/2022"]]
# real-udf: origin=github-spark id=pamepi_fix_date requires= wrapped=0 ported=0
# Source: https://github.com/PAMepi/PAMepi_scripts_datalake/blob/404673c8c0e7b45f0f5ddcba963310f3a15979db/Brazilian%20COVID-19%20data%20streaming/Data%20curation/myfunctions.py#L7-L19
# Repository: PAMepi/PAMepi_scripts_datalake @ commit 404673c8c0e7b45f0f5ddcba963310f3a15979db
# License: MIT
# Copied verbatim from Brazilian COVID-19 data streaming/Data curation/myfunctions.py (L7-L19); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf()

import re
from datetime import datetime


def corrige_data(col):
    try:
        if re.match(r'\d{2}/\d{2}/\d{4}', col):
            _ = datetime.strptime(col, '%d/%m/%Y').date()
            return _.strftime('%Y-%m-%d')
        elif re.match(r'\d{4}/\d{2}/\d{2}', col):
            _ = datetime.strptime(col, '%Y/%m/%d').date()
            return _.strftime('%Y-%m-%d')
        else:
            return col
    except TypeError:
        return col
