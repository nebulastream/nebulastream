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
We will implement the parser using a state-of-the-art parser generator (G2), such as GNU Bison or ANTLR, based on an existing SQL grammar file (G1).

# Non-Goals
We do not implement a parser for the existing functional query sytax, shown in the query above (for time reasons. We may later add one).

# (Optional) Solution Background
A parser is part of every database system with a language interface.
Most systems use parser generators over handwritten parsers.
Parser generators generate the the parser code based on the grammar.
One then must only transform the geric data stuctures of pared tokens into custom datastructed, for example representing SQL statements with a select, from, and where clasue.

Parsers of existing systems can be taken as a baseline already defining large parts of the targeted grammar and custim data structures.

# Our Proposed Solution
We want to base our implementation on the DuckDB parser (https://github.com/duckdb/duckdb/tree/main/third_party/libpg_query), which in turn is based on the PostgreSQL parser (https://github.com/pganalyze/libpg_query).
PostgreSQL's SQL dialect and implementation have a rich set of features and are widely used.
The DuckDB version is "stripped down and generally cleaned up to make it easier to modify and extend".

The DuckDB parser is integrated into DuckDB, i.e., it is no off-the-shelf library.
This is why we have to extract it.
In this process, we want to make it a standalone library.
Given a SQL query string, it will then output an object of type `SQLStatement`.
Each `SQLStatement` has a type `TYPE`, e.g., `SELECT_STATEMENT`, `INSERT_STATEMENT`.
Using the type information, we can cast it into derived classes, e.g., `SQLStatement`.
Derived classes are, in turn, tagged datastructures depending on the components of the query, e.g., whether it contains set operations or has subqueries.

To integrate this system-agnostic parser library into NES, we have to traverse the `SQLStatement`, extract the information, and generate a NES-specific query representation, i.e., `NES::Query` for a start.

# Proof of Concept
We started to extract a parsing library out of DuckDB.

# Alternatives
- discuss alternative approaches A1, A2, ..., including their advantages and disadvantages

# (Optional) Open Questions
- list relevant questions that cannot or need not be answered before merging
- create issues if needed

# (Optional) Sources and Further Reading
- [1] Arvind Arasu, Shivnath Babu, Jennifer Widom: The CQL continuous query language: semantic foundations and query execution. VLDB J. 15(2): 121-142 (2006).
  http://infolab.stanford.edu/~arvind/papers/cql-vldbj.pdf

# (Optional) Appendix
- provide here nonessential information that could disturb the reading flow, e.g., implementation details
