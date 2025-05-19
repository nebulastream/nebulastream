

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
