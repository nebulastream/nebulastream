# Numba runtime (vendored)

The C runtime that Numba-compiled code calls, from Numba (see `VERSION`), unchanged: the memory runtime NRT
(`core/runtime`), the typed dict and list (`cext`), and the Unicode type database (`_unicodetype_db.h`). NES compiles them
into the worker for INLINE UDFs compiled with Numba (`COMPILER NUMBA`), without Python: `cext/cext.h` includes `Python.h`
only for the types `Py_ssize_t` and `Py_hash_t`, which `../../python-shim/Python.h` defines. Licensed under the BSD
2-clause license in `LICENSE`.
