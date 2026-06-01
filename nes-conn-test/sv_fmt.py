# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""lldb summary for libstdc++ ``std::string_view``.

lldb's bundled formatters cover libc++'s ``std::__N::basic_string_view`` but
not libstdc++'s ``std::basic_string_view``. On a libstdc++ build (the
``local-libstd`` dev image) lldb therefore falls back to the raw
``const char* _M_str``, which is rendered as a NUL-terminated C-string — so a
view over binary/leading-NUL data (e.g. the conn-test ``small.bin`` fixture
``0x00..0xFF``) shows as an empty string even though ``size()`` is non-zero.

This provider reads ``_M_len`` bytes from ``_M_str`` and renders them
length-bounded with escapes, matching what lldb shows for libc++ natively.

Load it into CLion's lldb (which sources ``~/.lldbinit`` on session start)::

    command script import /abs/path/to/nes-conn-test/sv_fmt.py
"""

import lldb

_MAX = 256


def sv_summary(valobj, internal_dict):
    length = valobj.GetChildMemberWithName("_M_len").GetValueAsUnsigned(0)
    if length == 0:
        return '"" (len=0)'
    data = valobj.GetChildMemberWithName("_M_str").GetPointeeData(0, length)
    err = lldb.SBError()
    raw = data.ReadRawData(err, 0, min(length, _MAX))
    if err.Fail() or raw is None:
        return "<unreadable>"
    esc = raw.decode("latin-1").encode("unicode_escape").decode("ascii")
    tail = "..." if length > _MAX else ""
    return f'"{esc}{tail}" (len={length})'


def __lldb_init_module(debugger, internal_dict):
    debugger.HandleCommand(
        f'type summary add -x "^std::basic_string_view<.+>$" -F {__name__}.sv_summary'
    )
