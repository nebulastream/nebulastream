# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Mozilla/5.0+(Windows+NT+10.0;+Win64;+x64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/124.0.0.0+Safari/537.36"], ["Mozilla/5.0+(iPhone;+CPU+iPhone+OS+17_4+like+Mac+OS+X)+AppleWebKit/605.1.15+(KHTML,+like+Gecko)+Version/17.4+Mobile/15E148+Safari/604.1"], ["Mozilla/5.0+(compatible;+Googlebot/2.1;++http://www.google.com/bot.html)"], ["-"], ["Mozilla/5.0+(iPad;+CPU+OS+16_6+like+Mac+OS+X)+AppleWebKit/605.1.15+(KHTML,+like+Gecko)+Version/16.6+Mobile/15E148+Safari/604.1"], ["curl/8.5.0"], ["Mozilla/5.0%20(X11;%20Linux%20x86_64;%20rv:125.0)%20Gecko/20100101%20Firefox/125.0"]]
# real-udf: origin=streaming id=w3c_ua_safe_parse requires=user-agents wrapped=1 ported=0
# Wrapper: removed the @pandas_udf(...) decorator of parse_ua_fields (it needs PySpark and the module's struct schema); case_entry calls the per-row _safe_parse and returns its dict as JSON text.
# Imports: the original module's import that _safe_parse uses, and json for case_entry.

import json

from user_agents import parse as ua_parse

# Source: https://github.com/AhmedIkram05/w3c-etl-pipeline/blob/9b30e751e8f4dadd4c9eca2e26628b7f30a123bd/pipeline/spark/jobs/utils/ua_parser.py#L16-L87
# Repository: https://github.com/AhmedIkram05/w3c-etl-pipeline (commit 9b30e751e8f4dadd4c9eca2e26628b7f30a123bd)
# License: MIT
# System: Spark (Structured Streaming)
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def _safe_parse(ua_string: str) -> dict:
    """Parse a single UA string into a result dict.

    Returns a dict with keys matching the UDF return types:
    ``agent_type``, ``browser_name``, ``browser_version``,
    ``operating_system``, ``device_type``.
    """
    # Pandas passes NULL as NaN (a float) into a pandas_udf, so guard the
    # type before calling str methods. `not ua_string` does not catch NaN.
    if not isinstance(ua_string, str) or not ua_string.strip() or ua_string.strip() in ("-", "Unknown", ""):
        return {
            "agent_type": "Unknown",
            "browser_name": "Unknown",
            "browser_version": "Unknown",
            "operating_system": "Unknown",
            "device_type": "Unknown",
        }

    from urllib.parse import unquote_plus

    normalized = unquote_plus(ua_string.strip())
    ua = ua_parse(normalized)

    agent_type = "Crawler" if getattr(ua, "is_bot", False) else "Browser"
    browser_name = ua.browser.family or "Unknown"
    browser_version = ua.browser.version_string or "Unknown"
    os_name = ua.os.family or "Unknown"

    if getattr(ua, "is_mobile", False):
        device = "Mobile"
    elif getattr(ua, "is_tablet", False):
        device = "Tablet"
    elif getattr(ua, "is_pc", False):
        device = "Desktop"
    elif getattr(ua, "is_bot", False):
        device = "Bot"
    else:
        device = "Other"

    return {
        "agent_type": agent_type,
        "browser_name": browser_name,
        "browser_version": browser_version,
        "operating_system": os_name,
        "device_type": device,
    }


def parse_ua_fields(ua_series: pd.Series) -> pd.DataFrame:
    """Consolidated UA parsing UDF: one ``ua_parse`` call per row.

    Replaces five scalar ``parse_*`` UDFs that each re-parsed the same
    string; callers unpack fields from the struct. Output values match
    the scalar UDFs exactly.
    """
    parsed = ua_series.apply(_safe_parse)
    return pd.DataFrame(parsed.tolist(), index=ua_series.index, columns=_UA_STRUCT_SCHEMA.fieldNames())


def case_entry(ua_string):
    return json.dumps(_safe_parse(ua_string), sort_keys=True)
