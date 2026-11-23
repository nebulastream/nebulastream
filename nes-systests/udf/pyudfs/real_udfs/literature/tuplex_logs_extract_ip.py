# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /history/apollo/ HTTP/1.0\" 200 6245"], ["unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985"], ["burger.letters.com - - [01/Jul/1995:00:00:12 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 304 0"], ["d104.aa.net - - [01/Jul/1995:00:00:13 -0400] \"GET /shuttle/countdown/count.gif HTTP/1.0\" 200 40310"], ["piweba3y.prodigy.com - - [01/Jul/1995:00:00:14 -0400] \"GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0\" 404 -"], ["205.189.154.54 - - [01/Jul/1995:00:00:29 -0400] \"POST /cgi-bin/imagemap/countdown?99,176 HTTP/1.0\" 302 110"], ["pipe1.nyc.pipeline.com - - [01/Jul/1995:00:22:43 -0400] \"-\" 400 -"], ["ix-or10-06.ix.netcom.com - - [01/Jul/1995:00:00:58"]]
# real-udf: origin=literature id=tuplex_logs_extract_ip requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Wrapper: The benchmark script imports re at its top; the case adds the import before the UDF code.
import re

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/logs/runtuplex.py#L65-L70
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/logs/runtuplex.py, lines 65-70

def extract_ip(x):
    match = re.search(r"(^\S+) ", x['logline'])
    if match:
        return match[1]
    else:
        return ''


def case_entry(logline):
    return extract_ip({"logline": logline})
