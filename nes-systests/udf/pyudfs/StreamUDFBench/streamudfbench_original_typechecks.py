# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The StreamUDFBench UDFs jaccard and jsonparse as published, before streamudfbench_scalar.py adapted their type checks.

Codon decides isinstance(x, T) and type(x) at compile time and its json module returns a single json.Value type, so
these versions do not work under BRIDGE 'codon': jaccard fails to compile (type(x) == list), and jsonparse compiles its
isinstance checks to False. StreamUDFBenchJsonCodon.test pins both behaviours. Under CPython and PyPy they behave like
the adapted versions.
"""
import json


def _str(x):
    return x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x


def jaccard(arg1:str,arg2:str)->float:
    arg1 = _str(arg1)
    arg2 = _str(arg2)

    if arg1 is not None and arg2 is not None:
        try:
            r=json.loads(arg1)
            s=json.loads(arg2)
            rset=set([tuple(x) if type(x)==list else x for x in r])
            sset=set([tuple(x) if type(x)==list else x for x in s])
            return float(len( rset & sset ))/(len( rset | sset ))
        except:
            return None
    else:
        return None


def jsonparse(json_content: str,key1: str)->str:
    json_content = _str(json_content)
    key1 = _str(key1)

    try:
        data = json.loads(json_content)
        if isinstance(data, list):
            for item in data:
                return item.get(key1)
        elif isinstance(data, dict):
            return data.get(key1)
        else:
            return None
    except:
        return None
