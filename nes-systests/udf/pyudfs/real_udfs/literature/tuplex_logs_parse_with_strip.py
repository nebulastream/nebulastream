# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /history/apollo/ HTTP/1.0\" 200 6245"], ["unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985"], ["burger.letters.com - - [01/Jul/1995:00:00:12 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 304 0"], ["d104.aa.net - - [01/Jul/1995:00:00:13 -0400] \"GET /shuttle/countdown/count.gif HTTP/1.0\" 200 40310"], ["piweba3y.prodigy.com - - [01/Jul/1995:00:00:14 -0400] \"GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0\" 404 -"], ["205.189.154.54 - - [01/Jul/1995:00:00:29 -0400] \"POST /cgi-bin/imagemap/countdown?99,176 HTTP/1.0\" 302 110"], ["pipe1.nyc.pipeline.com - - [01/Jul/1995:00:22:43 -0400] \"-\" 400 -"]]
# real-udf: origin=literature id=tuplex_logs_parse_with_strip requires= wrapped=1 ported=0
# Wrapper: the UDF returns a dict; case_entry returns it as JSON with sorted keys.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/logs/runtuplex.py#L121-L187
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/logs/runtuplex.py, lines 121-187

def ParseWithStrip(x):
    y = x

    i = y.find(" ")
    ip = y[:i]
    y = y[i + 1 :]

    i = y.find(" ")
    client_id = y[:i]
    y = y[i + 1 :]

    i = y.find(" ")
    user_id = y[:i]
    y = y[i + 1 :]

    i = y.find("]")
    date = y[:i][1:]
    y = y[i + 2 :]

    y = y[y.find('"') + 1 :]

    method = ""
    endpoint = ""
    protocol = ""
    failed = False
    if y.find(" ") < y.rfind('"'):
        i = y.find(" ")
        method = y[:i]
        y = y[i + 1 :]

        i = y.find(" ")  # needs to be any whitespace
        endpoint = y[:i]
        y = y[i + 1 :]

        i = y.rfind('"')
        protocol = y[:i]
        protocol = protocol[protocol.rfind(" ") + 1 :]
        y = y[i + 2 :]
    else:
        failed = True
        i = y.rfind('"')
        y = y[i + 2 :]

    i = y.find(" ")
    response_code = y[:i]
    content_size = y[i + 1 :]

    if not failed:
        return {"ip": ip,
                "client_id": client_id,
                "user_id": user_id,
                "date": date,
                "method": method,
                "endpoint": endpoint,
                "protocol": protocol,
                "response_code": int(response_code),
                "content_size": 0 if content_size == '-' else int(content_size)}
    else:
        return {"ip": "",
                "client_id": "",
                "user_id": "",
                "date": "",
                "method": "",
                "endpoint": "",
                "protocol": "",
                "response_code": -1,
                "content_size": -1}


import json


def case_entry(logline):
    return json.dumps(ParseWithStrip(logline), sort_keys=True)
