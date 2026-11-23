# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""UDFs for udf/HiPyFallback.test (COMPILER HIPY): HiPy compiles the string handling of each natively and leaves the rest to
CPython, which the engine calls through the CPython bridge."""

import datetime
import hashlib
import json


def bucket(s):
    """Native: strip, lower, len and the branch. CPython: hashlib, encode, hexdigest, the slice, int(..., 16) and the sum."""
    t = s.strip().lower()
    if len(t) == 0:
        return 0
    h = hashlib.md5(t.encode()).hexdigest()
    return len(t) * 1000 + int(h[0:4], 16)


def digest(s):
    """A str that CPython computes is the result."""
    return hashlib.sha1(s.encode()).hexdigest()[:10] + ":" + str(len(s))


def maybe_key(s):
    """CPython returns None for a document without the key: the result is NULL."""
    if s.startswith("{"):
        return json.loads(s).get("key")
    return None


def shift_minutes(minutes):
    """A keyword argument in a call that HiPy's datetime stand-in leaves to CPython."""
    return int(datetime.timedelta(minutes=minutes).total_seconds())


def parse_strict(s):
    """json.loads raises in CPython for text that is not JSON: the query fails."""
    return json.loads(s)["key"]
