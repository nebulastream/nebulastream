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
- describe relevant prior work, e.g., issues, PRs, other projects

# Our Proposed Solution
- start with a high-level overview of the solution
- explain the interface and interaction with other system components
- explain a potential implementation
- explain how the goals G1, G2, ... are addressed
- use diagrams if reasonable

# Proof of Concept
- demonstrate that the solution should work
- can be done after the first draft

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
