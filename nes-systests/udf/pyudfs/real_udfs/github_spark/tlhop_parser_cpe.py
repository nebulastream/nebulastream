# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["cpe:2.3:a:apache:http_server:2.4.49:*:*:*:*:*:*:*"], ["cpe:/a:openbsd:openssh:7.4,cpe:/o:linux:linux_kernel"], ["cpe:2.3:a:nginx:nginx:1.18.0:*:*:*:*:*:*:*,cpe:2.3:a:nginx:nginx:1.20.1:*:*:*:*:*:*:*"], ["cpe:2.3:o:microsoft:windows_10:1909:*:*:*:*:*:*:*,cpe:/h:cisco:asa_5505:9.1"], ["cpe:/a:php:php:7.4.3"]]
# real-udf: origin=github-spark id=tlhop_parser_cpe requires= wrapped=1 ported=0
# Wrapper: the UDF returns an array of structs; case_entry returns it as JSON text.
# Source: https://github.com/lucasmsp/tlhop-library/blob/c2943f775bbc9bc99705d5db2495dea3f6528fe3/tlhop/library.py#L746-L813
# Repository: lucasmsp/tlhop-library @ commit c2943f775bbc9bc99705d5db2495dea3f6528fe3
# License: Apache-2.0
# Copied verbatim from tlhop/library.py (L746-L813); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(returnType=cpe_schema)

def parser_cpe_udf(text):
    '''
        Method that receive a cpe object and parses it.
        :param 1° - text: cpe data

        :returns: cpe_info
        :rtype: Dictionary Array or Empty Array
    '''
    results = {}
    if text:
        if isinstance(text, str):
            text = text.split(",")

        for cpe in text:
            part = None

            if "cpe:2.3" in cpe:
                _, _, part, vendor, *cpe = cpe.split(":")
                if len(cpe) == 1:
                    product = cpe[0]
                    version = None
                    update = ""
                elif len(cpe) > 1:
                    product = cpe[0]
                    version = cpe[1]
                    update = cpe[2]
                else:
                    product = None
                    version = None
                    update = ""

            elif "cpe:/" in cpe:
                cpe = cpe.replace("cpe:/", "").split(":")
                part, vendor, product = cpe[0], cpe[1], cpe[2]
                if len(cpe) > 3:
                    version = cpe[3]
                    update = ""
                else:
                    version = None
                    update = ""

            if part:
                if "a" == part:
                    part = "Application"
                elif "h" == part:
                    part = "Hardware"
                elif "o" == part:
                    part = "Operating System"

                key = str({"cpe_type": part, "cpe_vendor": vendor, "cpe_product": product})

                if key not in results:
                    results[key] = {"cpe_type": part, "cpe_vendor": vendor, "cpe_product": product, "cpe_versions": set()}

                if version:    
                    # if version == "-":
                    #     version = "*"
                    # if update == "-":
                    #     update = "*"
                    r = ":".join([version, update])
                    results[key]["cpe_versions"].add(r)

        for e in results:
            results[e]["cpe_versions"] = sorted(list(results[e]["cpe_versions"]))
        results = [v for v in results.values()]

    return results


import json


def case_entry(text):
    return json.dumps(parser_cpe_udf(text), sort_keys=True)
