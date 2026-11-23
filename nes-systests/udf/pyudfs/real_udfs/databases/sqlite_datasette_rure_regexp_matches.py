# codon-fuzz: entry=regexp_matches args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["(?P<key>\\w+)=(?P<value>\\w+)", "color=red size=42 fit=slim"], ["(?P<year>\\d{4})-(?P<month>\\d{2})", "Jan 2019-01, Feb 2019-02 and Mar 2019-03"], ["(?P<user>[\\w.]+)@(?P<domain>[\\w.]+)", "Contact simon@example.com or ops@datasette.io"], ["(?P<tag>#\\w+)", "Loving #datasette and #sqlite today"], ["(?P<n>\\d+)", "no digits here"]]
# real-udf: origin=databases id=sqlite_datasette_rure_regexp_matches requires=rure wrapped=0 ported=0
# Source: https://github.com/simonw/datasette-rure/blob/86492dcd42d0e7e5f8706eac31532357a1855b7d/datasette_rure/__init__.py#L39
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
def regexp_matches(pattern, input):
    return json.dumps([m.groupdict() for m in compiled_regex(pattern).finditer(input)])
