# schema-firewall

Diagnostic scaffold, not a shipped mechanism. Delete once `nes-physical-operators` is Schema-free.

`nes-schema` is an INTERFACE target, so its include directory propagates through every `PUBLIC`
link. `nes-physical-operators` inherits it from `nes-nautilus`, `nes-sources`, `nes-sinks` and
`nes-logical-operators` — all of which take `Schema` by value in their own public headers, so none
of them can demote the dependency to `PRIVATE`. There is no CMake configuration that hands
`nes-physical-operators` the include directory for `Functions/LogicalFunction.hpp` but withholds it
for `Schema/SchemaFwd.hpp`.

This directory is added `BEFORE` on `nes-physical-operators` only, so `Schema/*.hpp` resolves here
first and hard-errors. That reproduces the end state of the split and turns it into a compile-error
worklist.

Once the last `Schema` use is gone from `nes-physical-operators`, drop this directory and the
`target_include_directories(... BEFORE ...)` line — the target simply stops including `Schema/`.
