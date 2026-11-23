# codon-fuzz: entry=makeIIIF args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["https://chroniclingamerica.loc.gov/data/batches/batch_dlc_elf_ver01/data/sn83030214/00175041217/1865041401/0099.jp2"], ["https://panewsarchive.psu.edu/data/batches/batch_pst_fenske_ver02/data/sn85054967/0021305183A/1880010101/0001.jp2"], ["https://chroniclingamerica.loc.gov/data/batches/batch_nn_euclid_ver01/data/sn83045462/00280609904/1922120301/0123.jp2"], ["https://chroniclingamerica.loc.gov/data/batches/batch_ca_dove_ver01/data/sn85066387/00211101379/1900060101/0045.jp2"]]
# real-udf: origin=github-spark id=viraltexts_make_iiif requires= wrapped=0 ported=0
# Source: https://github.com/ViralTexts/vt-passim/blob/5ba7d3c4f586e424b7371d7b7a39e41fc025daec/scripts/ndnp-page-merge.py#L13-L18
# Repository: ViralTexts/vt-passim @ commit 5ba7d3c4f586e424b7371d7b7a39e41fc025daec
# License: Apache-2.0
# Copied verbatim from scripts/ndnp-page-merge.py (L13-L18); only the module's own import lines that the code uses are prepended.

import argparse, re
from urllib.parse import quote, urlparse


def makeIIIF(fname):
    if fname == None or fname == '':
        return None
    domain, file = re.split(r'/data/batches/', fname)
    mid = '/images/iiif/' if domain.find('panewsarchive') == -1 else '/iiif/'
    return domain + mid + quote(file, safe='')
