# codon-fuzz: entry=case_entry args=INT64,VARSIZED,INT64 returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[72, "MID-101", 1716202800000], [89, "MID-102", 1716202801000], [90, "MID-103", 1716202802000], [104, "MID-101", 1716202803000], [-5, "MID-104", 1716202804000], [65, "MID-105", 1716202805000]]
# real-udf: origin=streaming id=quix_should_alert requires= wrapped=1 ported=0
# Wrapper: should_alert returns True or None (falsy) and is used as a filter; case_entry passes headers=None and returns bool(result).

# Source: https://github.com/quixio/quix-streams/blob/e7fd055b09a0a6d458213be69c40064869ba3e82/docs/tutorials/anomaly-detection/tutorial_app.py#L93-L96
# Repository: https://github.com/quixio/quix-streams (commit e7fd055b09a0a6d458213be69c40064869ba3e82)
# License: Apache-2.0
# System: Quix Streams
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def should_alert(window_value: int, key, timestamp, headers):
    if window_value >= 90:
        print(f"Alerting for MID {key}: Average Temperature {window_value}")
        return True


def case_entry(window_value, key, timestamp):
    return bool(should_alert(window_value, key, timestamp, None))
