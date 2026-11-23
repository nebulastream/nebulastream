# codon-fuzz: entry=makeURL args=VARSIZED,VARSIZED,VARSIZED,VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["https://chroniclingamerica.loc.gov/lccn/sn83030214/1865-04-14/ed-1/", "/lccn/sn83030214", "1865-04-14", "1", "3"], ["https://chroniclingamerica.loc.gov/lccn/sn83045462/1922-12-03/ed-1/", "/lccn/sn83045462", "1922-12-03", "1", "12"], ["https://panewsarchive.psu.edu/lccn/sn85054967/1880-01-01/ed-2/", "/lccn/sn85054967", "1880-01-01", "2", "1"], ["http://chroniclingamerica.loc.gov:80/lccn/sn85066387/1900-06-01/ed-1/", "/lccn/sn85066387", "1900-06-01", "1", "4"]]
# real-udf: origin=github-spark id=viraltexts_make_url requires= wrapped=0 ported=0
# Source: https://github.com/ViralTexts/vt-passim/blob/5ba7d3c4f586e424b7371d7b7a39e41fc025daec/scripts/ndnp-page-merge.py#L9-L11
# Repository: ViralTexts/vt-passim @ commit 5ba7d3c4f586e424b7371d7b7a39e41fc025daec
# License: Apache-2.0
# Copied verbatim from scripts/ndnp-page-merge.py (L9-L11); only the module's own import lines that the code uses are prepended.

from urllib.parse import quote, urlparse


def makeURL(issue, series, date, ed, seq):
    res = urlparse(issue)
    return f'{res.scheme}://{res.netloc}{series}/{date}/ed-{ed}/seq-{seq}/'
