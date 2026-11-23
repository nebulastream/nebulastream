# Codon overlays for pure-Python libraries

Codon UDFs can import pure-Python libraries, e.g. CPython's standard library, from the directories in
`NES_CODON_PYTHON_PATH`. Most library code compiles as it is; missing Python features are fixed in the Codon fork.
An overlay is the last resort, for code that is native in CPython or that does not fit Codon's static types (for
example an object whose method is replaced per object).

`NES_CODON_PYTHON_OVERLAYS` (':'-separated) names overlay directories. When Codon loads a library module, NES looks
for a file at the same path relative to an overlay directory, e.g. `cpython-3.14/ipaddress.py` for
`/usr/lib/python3.14/ipaddress.py`. Codon merges its top-level statements into the module before type checking:

- A definition (`def`, `class`, assignment to a name, `import`) replaces the module's first statement that binds the
  same name, in its place, so the order of module initialization stays.
- `__nes_remove__ = ["name", ...]` removes the module's statements that bind these names (e.g. an import of a C
  module).
- A class decorated with `@__nes_patch__` replaces or adds single members (methods, class variables) of the module's
  class of that name; the rest of the class stays.
- Other statements are appended to the module.

Each overlay file states at its top why it exists. An overlay follows one CPython version (the directory name).

| Overlay | Reason |
|---|---|
| `cpython-3.14/ipaddress.py` | `IPv4Network`/`IPv6Network.__init__` replace the method `hosts` per object with callables of different types |

`test/check.sh BUILD_DIR` checks the merge (removal, replacement, class patch, appended definition).
