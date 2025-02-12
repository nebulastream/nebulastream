# The Problem
Currently, NES has a REST query interface for users, who can submit query strings, such as the following.
```
Query::from("default_source")
    .filter(Attribute("id") < 16)
    .sink(FileSinkDescriptor::create("[GENERATED_IN_TEST]", "CSV_FORMAT", "APPEND"));
```

For processing, this string is basically written into a c++ file and compiled.

We want a streaming SQL frontend.

Besides, the current approach has the following drawbacks:
- The compilation is slow, taking about 1 second, which is too long, for example, for running multiple integration tests.
- The process is insecure because users could add arbitrary c++ code to the query string, which would be compiled.

# Goals
We want to add a new parser, which:
- (G1) supports an SQL-like declarative streaming syntax (see CQL paper [1]),
- (G2) is reasonably efficient (i.e., below a millisecond for a (TPC-H-style complex) query, and
- (G3) transforms the query into the NebulaStream-internal format (see the query above).
- (G4) is hackable/maintainable as we want to extend it with (partly) to-be-developed SQL streaming extensions

# Non-Goals
- (NG1) Highest performance is no primary goal.
- (NG2) Full SQL support and a discussion of desired SQL features are non-goals of this DD.
- (NG3) A specific streaming extension syntax will be discussed in another DD.

# Solution Background
A parser is part of every database system with a language interface.
Most systems use parser generators (e.g., ANTLR or Flex/Bison) over handwritten parsers.
Parser generators generate the parser code based on the grammar.
One then must only transform the generic data structures of parsed tokens into custom data structures, for example, representing SQL statements with a select, from, and where clause.

Parsers of existing systems can be taken as a baseline that already defines large parts of the targeted grammar and custom data structures.
However, one can also start with a simpler, potentially easier-to-extend or maintain grammar.

After looking into alternative approaches and discussing them, we find (on the one hand) that there is no evident best approach to choose (see advantages, disadvantages, and open questions).
On the other hand, we could base our parser on different alternatives that were investigated.

The Firebolt paper (see [2], Section 2.2) also discusses the team's considerations (language, simplicity vs. SQL support, usability, specific projects) when they chose the parser.


# Our Proposed Solution
Based on the research group's experience and our discussion, we voted to use an ANTLR-based parser over the alternatives below.
We would like to start with a stripped-down grammar (over a full-fledged SQL support).
To get this grammar, we want to use a solid base grammar (e.g., with expression support) and delete features over defining our grammar from scratch.

# Proof of Concept
A former student integrated an ANTLR parser into NES https://github.com/nebulastream/nebulastream/tree/ba-niklas-NESSQL/nes-core/src/Parsers
- grammar: https://github.com/nebulastream/nebulastream/blob/ba-niklas-NESSQL/nes-core/src/Parsers/NebulaSQL/gen/NebulaSQL.g4
-  \- This parser has limited SQL support, missing many SQL features, e.g., a complete set of set operations.
-  \- Also for existing features, it has some shortcomings, which are not documented, such as no case mix (e.g., "SeleCT") for keywords.
-  \+ It contains streaming extensions, also tailored to the old NES system, e.g., MQTT sink.
- @alepping and @ls-1801 have integrated this parser with PR #424

# Alternatives
Basically, we could base our parser on other systems.
We looked into some alternatives more deeply and list them here:

### A1: The Extracted PostgreSQL Parser
https://github.com/pganalyze/libpg_query
- PostgreSQL's SQL dialect and implementation have a rich set of features and are widely used.

### A2: The DuckDB Parser
https://github.com/duckdb/duckdb/tree/main/third_party/libpg_query
- based on the PostgreSQL parser, written in C++
- "stripped down and generally cleaned up to make it easier to modify and extend" (see link above)
- this parser is integrated into DuckDB, i.e., it is no off-the-shelf library; we would have to extract it, e.g., in a system-agnostic library: we started doing this in the following repositories: https://github.com/nebulastream/sql-parser, https://github.com/klauck/nebulastream-frontend

### A3: Hyrise parser
https://github.com/hyrise/sql-parser
- A c++ parser library, originally built for the Hyrise database system
- \+ A standalone, ready-to-use library.
- \- No full SQL support and minor bugs (see also https://github.com/hyrise/sql-parser/issues)
- \+ Showed usefulness in Firebolt (see Firebolt paper [2])

### Other Alternatives
- Google's zetasql https://github.com/google/zetasql
- Datafusion's SQL parser (in Rust) https://github.com/apache/datafusion-sqlparser-rs

# Open Questions
### Which specific ANTLR grammar file to base on:
- the NES one has some limitations (see above)
- The ANTLR project provides grammars of different systems, e.g., PostgreSQL, SQLite  https://github.com/antlr/grammars-v4/tree/master/sql
    -  \+ It supports a rich set of SQL features.
    -  \- Not well documented and tested.
    -  \- Only grammar file, but no custom data structures (see `SQLStatement` above)

### Future difficulties
When extending the grammar for streaming, e.g., Windows specifications in the from clause, we recently encountered parser conflicts, e.g., shift/reduce conflicts (https://www.gnu.org/software/bison/manual/html_node/Shift_002fReduce.html).
The appearance, naturally, depends on the used feature and syntax, e.g., using only brackets (e.g., `[RANGE 10 HOUR]`) or a `WINDOW` keyword for the definition.
It is open to us how much effort it is to integrate every streaming extension without conflicts.


# Sources and Further Reading
- [1] Arvind Arasu, Shivnath Babu, Jennifer Widom: The CQL continuous query language: semantic foundations and query execution. VLDB J. 15(2): 121-142 (2006).
  http://infolab.stanford.edu/~arvind/papers/cql-vldbj.pdf
- [2] Mosha Pasumansky, Benjamin Wagner: Assembling a Query Engine From Spare Parts. CDMS@VLDB (2022).
  https://cdmsworkshop.github.io/2022/Proceedings/ShortPapers/Paper1_MoshaPasumansky.pdf

