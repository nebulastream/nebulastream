# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""UDFBench cleandate for LPython. Changes: LPython types, no `self`, no try/except, `"-" in pubdate` (not
implemented for str) as find, None is "NULL"."""
from lpython import ccallable, i32


def cleandate(pubdate: str) -> str:
    splitnum: i32
    pubdate_split: list[str]
    if pubdate != "":
        if pubdate.find("-") >= 0:
            splitnum = pubdate.count("-")
            pubdate_split = pubdate.split("-")
            if splitnum == 1:
                return pubdate_split[0] + "/" + pubdate_split[1] + "/" + "01"
            elif splitnum == 2:
                return pubdate_split[0] + "/" + pubdate_split[1] + "/" + pubdate_split[2]
            else:
                return "NULL"
        elif pubdate.find("/") >= 0:
            splitnum = pubdate.count("/")
            pubdate_split = pubdate.split("/")
            if splitnum == 1:
                return pubdate_split[0] + "/" + pubdate_split[1] + "/" + "01"
            elif splitnum == 2:
                return pubdate_split[0] + "-" + pubdate_split[1] + "-" + pubdate_split[2]
            else:
                return "NULL"
        else:
            return "NULL"
    else:
        return "NULL"


@ccallable
def udf_entry(pubdate: str) -> str:
    copy: str = pubdate  # LPython miscompiles a @ccallable function that passes its str argument on
    return cleandate(copy)
