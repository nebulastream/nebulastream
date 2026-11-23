# codon-fuzz: entry=regexp args=VARSIZED,VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["\\d{4}-\\d{2}-\\d{2}", "Released on 2019-03-22 in Europe"], ["^https?://", "https://datasette.io/plugins"], ["(?i)error", "2024-03-15 14:22:05 WARN disk 91% full"], ["[A-Z]{2}\\d{5}", "Order shipped to CA94105"], ["\\bcat\\b", "concatenate"]]
# real-udf: origin=databases id=sqlite_datasette_rure_regexp requires=rure wrapped=0 ported=0
# Source: https://github.com/simonw/datasette-rure/blob/86492dcd42d0e7e5f8706eac31532357a1855b7d/datasette_rure/__init__.py#L29
# Commit: 86492dcd42d0e7e5f8706eac31532357a1855b7d
# License: Apache-2.0
# Shared helpers (cached compiled regex, none_on_exception decorator) included above the UDF.

import functools
import threading
import json
import rure


@functools.lru_cache(maxsize=128)
def _compiled_regex(threadid, pattern):
    return rure.compile(pattern)


def compiled_regex(pattern):
    return _compiled_regex(threading.get_ident(), pattern)


def none_on_exception(fn):
    @functools.wraps(fn)
    def inner(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            return None

    return inner


@none_on_exception
def regexp(pattern, input):
    return 1 if compiled_regex(pattern).search(input) else 0
