# The Problem
Currently, NES has a REST query interface for users, who can submit query strings, such as the following.
```
Query::from("default_source")
    .filter(Attribute("id") < 16)
    .sink(FileSinkDescriptor::create("[GENERATED_IN_TEST]", "CSV_FORMAT", "APPEND"));
```

For processing, this string is basically written into a c++ file and compiled.
This approach has the following drawbacks:
- The compilation is slow, taking about 1 second, which is too long, for example, for running multiple integration tests.
- The process is insecure because users could add arbitrary c++ code to the query string, which would be compiled.

# Goals
We want to add a new parser, which (G1) checks the grammar and (G2) is reasonably efficient.
We want a declarative query syntax similar to CQL [1].
Therefore, we will extend an existing SQL grammar (file) (G1), which already correctly implements a rich set of SQL syntax, e.g., aliases, set and date operations.
In this process, we will use a state-of-the-art parser generator (G2), such as GNU Bison or ANTLR.

# Non-Goals
We do not implement a parser for the existing functional query sytax, shown in the query above (for time reasons. We may later add one).

# Solution Background
A parser is part of every database system with a language interface.
Most systems use parser generators over handwritten parsers.
Parser generators generate the parser code based on the grammar.
One then must only transform the geric data stuctures of pared tokens into custom datastructed, for example representing SQL statements with a select, from, and where clasue.

Parsers of existing systems can be taken as a baseline already defining large parts of the targeted grammar and custom data structures.

# Our Proposed Solution
We want to base our implementation on the DuckDB parser (https://github.com/duckdb/duckdb/tree/main/third_party/libpg_query), which in turn is based on the PostgreSQL parser (https://github.com/pganalyze/libpg_query).
PostgreSQL's SQL dialect and implementation have a rich set of features and are widely used.
The DuckDB version is "stripped down and generally cleaned up to make it easier to modify and extend".

The DuckDB parser is integrated into DuckDB, i.e., it is no off-the-shelf library.
This is why we have to extract it.
In this process, we want to make it a standalone library.
Given an SQL query string, it will then output an object of type `SQLStatement`.
Each `SQLStatement` has a type `TYPE`, e.g., `SELECT_STATEMENT`, `INSERT_STATEMENT`.
Using the type information, we can cast it into derived classes, e.g., `SelectStatement`, `InsertStatement`.
Derived classes are, in turn, tagged datastructures depending on the components of the query, e.g., whether it contains set operations or has subqueries.

To integrate this system-agnostic parser library into NES, we have to traverse the `SQLStatement`, extract the information, and generate a NES-specific query representation, i.e., `NES::Query` for a start.

# Proof of Concept
We started to extract a parsing library out of DuckDB.

# Alternatives
Basically, we could base our parser on other systems.
The PostgreSQL parser implements many SQL features and its dialect is widely used.
We looked into some alternatives more deeply and list them here:

### A1: Old NES parser:
https://github.com/nebulastream/nebulastream/blob/ba-niklas-NESSQL/nes-core/src/Parsers/NebulaSQL/gen/NebulaSQL.g4
- This parser is based on ANTLR and was implemented in scope of a Bachelor's thesis.
-  \- It has limited SQL support, missing many SQL features, e.g., a complete set of set operations.
-  \- Also for existing features, it has some shortcomings, which are not documented, such as no case mix (e.g., "SeleCT") for keywords.
-  \+ It contains streaming extensions, also tailored to the old NES system, e.g., MQTT sink.
-  \+ There is a POC integration into the old NES (which is rather straight forward).

### A2: Use existing ANTLR grammar files
https://github.com/antlr/grammars-v4/tree/master/sql
- The ANTLR project provides grammars of different systems, e.g., PostgreSQL, SQLite
-  \+ It supports a rich set of SQL features.
-  \- Not well documented and tested.
-  \- Only grammar file, but no custom data structures (see `SQLStatement` above)

### A3: Hyrise parser
https://github.com/hyrise/sql-parser
- A c++ parser library, originally built for the Hyrise database system
- \+ A standalone, ready-to-use library.
- \- No full SQL support and minor bugs (see also https://github.com/hyrise/sql-parser/issues)

# Open Questions
When extending the grammar for streaming, e.g., Windows specifications in the from clause, we recently encountered parser conflicts, e.g., shift/reduce conflicts (https://www.gnu.org/software/bison/manual/html_node/Shift_002fReduce.html).
The appearance, naturally, depends on the used feature and syntax, e.g., using only brackets (e.g., `[RANGE 10 HOUR]`) or a `WINDOW` keyword for the definition.
It is open to us how much effort it is to integrate every streaming extension without conflicts.

# Sources and Further Reading
- [1] Arvind Arasu, Shivnath Babu, Jennifer Widom: The CQL continuous query language: semantic foundations and query execution. VLDB J. 15(2): 121-142 (2006).
  http://infolab.stanford.edu/~arvind/papers/cql-vldbj.pdf


