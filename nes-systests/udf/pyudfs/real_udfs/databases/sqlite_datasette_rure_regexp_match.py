# codon-fuzz: entry=regexp_match args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["(\\d{4})-\\d{2}-\\d{2}", "2019-03-22 release"], ["https?://([^/]+)/", "https://datasette.io/plugins/datasette-rure"], ["(\\w+)@", "simon@example.com"], ["v(\\d+\\.\\d+)", "v1.4.2 tagged"], ["(GET|POST) ", "POST /api/v1/items HTTP/1.1"]]
# real-udf: origin=databases id=sqlite_datasette_rure_regexp_match requires=rure wrapped=0 ported=0
# Source: https://github.com/simonw/datasette-rure/blob/86492dcd42d0e7e5f8706eac31532357a1855b7d/datasette_rure/__init__.py#L34
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
def regexp_match(pattern, input, index=1):
    return compiled_regex(pattern).match(input).group(index)
