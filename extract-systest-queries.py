# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path
import sys

if len(sys.argv) < 20:
    print("run with $(git ls-files *.test) as arg")
    sys.exit(1)

queries = set()
for file in sys.argv[1:]:
    in_query = False
    query = ""
    for line in Path(file).read_text().split("\n"):
        if not in_query and line.startswith("SELECT"):
            in_query = True
        if in_query and line.startswith("----"):
            queries.add(query.strip())
            query = ""
            in_query = False

        line = line.split("#")[0]

        if in_query:
            query += " " + line.strip()

for i, q in enumerate(queries):
    Path(f"{i:05}.systesql").write_text(q)
