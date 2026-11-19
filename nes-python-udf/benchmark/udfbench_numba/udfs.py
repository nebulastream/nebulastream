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
"""The UDFBench scalar UDFs and their inputs, as the INLINE queries of StreamUDFBenchScalarUdfs.test use them.

Each UDF reads one or two columns of a tiny StreamUDFBench file (tab-separated, no quoting, an empty field is NULL). The
functions are the unchanged UDFBench files in nes-systests/udf/pyudfs/StreamUDFBench/scalar; they take UDFBench's `self`
first, for which None stands in. A NULL argument is not passed to them: the query result is NULL, as in the INLINE entry
points (udfbench_scalar_inline.py), except where these return something else for None.
"""

import pathlib
import sys

REPOSITORY = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPOSITORY / "nes-systests/udf/pyudfs"))

DATA = pathlib.Path("/dev/shm/streamudfbench-data/large/streamudfbench/tiny")

# The columns of each source, from the CREATE LOGICAL SOURCE statements of StreamUDFBenchScalarUdfs.test.
SOURCES = {
    "artifacts": ("artifacts_15M.csv", ["id", "title", "publisher", "journal", "date", "year", "access_mode", "embargo_end_date",
                                        "delayed", "authors", "source", "abstract", "type", "peer_reviewed", "green", "gold", "ts"]),
    "artifact_abstracts": ("artifact_abstracts_13M.csv", ["id", "abstract", "ts"]),
    "artifact_authorlists": ("artifact_authorlists_4M.csv", ["id", "authorlist", "ts"]),
    "artifact_charges": ("artifact_charges_32K.csv", ["id", "amount", "currency", "ts"]),
    "projects_artifacts": ("projects_artifacts_16M.csv", ["id", "artifactid", "provenance", "funder", "fundinginfo", "ts"]),
    "query2json": ("query2json_3M.csv", ["id", "ts"]),
}

# UDF name -> (source, argument columns, argument types), as in the queries. A column "='text'" is a string constant.
UDFS = {
    "addnoise": ("artifacts", ["authors"], ["int"]),
    "clean": ("artifact_authorlists", ["authorlist"], ["str"]),
    "cleandate": ("artifacts", ["date"], ["str"]),
    "converttoeuro": ("artifact_charges", ["amount", "currency"], ["float", "str"]),
    "extractclass": ("projects_artifacts", ["fundinginfo"], ["str"]),
    "extractcode": ("projects_artifacts", ["fundinginfo"], ["str"]),
    "extractday": ("artifacts", ["date"], ["str"]),
    "extractfunder": ("projects_artifacts", ["fundinginfo"], ["str"]),
    "extractid": ("projects_artifacts", ["fundinginfo"], ["str"]),
    "extractmonth": ("artifacts", ["date"], ["str"]),
    "extractprojectid": ("projects_artifacts", ["fundinginfo"], ["str"]),
    "extractyear": ("artifacts", ["date"], ["str"]),
    "filterstopwords": ("artifact_abstracts", ["abstract"], ["str"]),
    "frequentterms": ("artifact_abstracts", ["abstract"], ["str"]),
    "jaccard": ("artifact_authorlists", ["authorlist", "='[\"Guy Boucher\"]'"], ["str", "str"]),
    "jpack": ("artifacts", ["title"], ["str"]),
    "jsoncount": ("artifact_authorlists", ["authorlist"], ["str"]),
    "jsonparse": ("query2json", ["id", "='doi'"], ["str", "str"]),
    "jsort": ("artifact_authorlists", ["authorlist"], ["str"]),
    "jsortvalues": ("artifact_authorlists", ["authorlist"], ["str"]),
    "keywords": ("artifact_abstracts", ["abstract"], ["str"]),
    "log_10": ("artifact_charges", ["amount"], ["float"]),
    "lowerize": ("artifacts", ["title"], ["str"]),
    "removeshortterms": ("artifact_authorlists", ["authorlist"], ["str"]),
    "stem": ("artifact_abstracts", ["abstract"], ["str"]),
}

# frequentterms takes N, a constant in the query.
FREQUENTTERMS_N = 5


def convert(value, kind):
    if value == "":
        return None
    if kind == "int":
        return int(value)
    if kind == "float":
        return float(value)
    return value


def inputs(name, limit=None):
    """The argument tuples of a UDF, one per row, with None for NULL."""
    source, columns, kinds = UDFS[name]
    file_name, schema = SOURCES[source]
    constants = {column: column[2:-1] for column in columns if column.startswith("='")}
    indices = [None if column in constants else schema.index(column) for column in columns]
    rows = []
    with open(DATA / file_name, encoding="utf-8", newline="\n") as data:
        for line in data:
            fields = line.rstrip("\n").split("\t")
            rows.append(tuple(constants[column] if index is None else convert(fields[index], kind)
                              for column, index, kind in zip(columns, indices, kinds)))
            if limit is not None and len(rows) >= limit:
                break
    if name == "frequentterms":
        rows = [(row[0], FREQUENTTERMS_N) for row in rows]
    return rows


def function(name):
    """The unchanged UDFBench function."""
    module = __import__(f"StreamUDFBench.scalar.{name}", fromlist=[name])
    return getattr(module, name)
