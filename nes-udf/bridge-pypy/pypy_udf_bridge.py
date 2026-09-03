# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# PyPy bridge implementing the NebulaStream scalar-UDF C ABI (UdfAbi.h), via cffi's embedding mode.
# Same ABI, same marshalling semantics as CPythonUdfBridge.cpp -- ported to Python because PyPy has no
# CPython-style Py_Initialize embedding API, only cffi embedding. Run this script with pypy3 to
# generate the C source CMake then compiles into libnes-pypy-udf-bridge.so.
#
# The GIL-equivalent locking cffi does internally per call; only the handle registry needs our own lock.

import sys

import cffi

ffibuilder = cffi.FFI()

ffibuilder.embedding_api("""
    void *malloc(size_t);

    int initialize_udf(const char *entrypoint, int argc, const int *arg_type_codes,
                        int return_type_code, char **errormessage);
    int execute_udf_row(int handle, const void *const *arg_values, const long long *arg_lens,
                         const int *arg_nulls, void *result_scalar, char **result_string,
                         long long *result_string_len, int *result_null, char **errormessage);
    void cleanup_udf(int handle);
""")

ffibuilder.set_source("nes_pypy_udf_bridge", "")

ffibuilder.embedding_init_code("""
    from nes_pypy_udf_bridge import ffi
    import importlib
    import os
    import sys
    import threading

    # Opt-in venv for third-party packages, mirroring CPythonUdfBridge.cpp's initPythonOnce. Unset (the
    # default) leaves system-local PyPy/site-packages exactly as before. Scan for the venv's one
    # version-specific dir instead of assuming it matches this bridge's own PyPy build -- a pure-Python
    # venv then works regardless of version drift; a compiled extension inside still needs a matching ABI.
    _venv = os.environ.get("NES_UDF_VENV", "")
    if _venv:
        _venv_lib = os.path.join(_venv, "lib")
        for _entry in sorted(os.listdir(_venv_lib)) if os.path.isdir(_venv_lib) else []:
            if _entry.startswith(("python", "pypy")) and os.path.isdir(os.path.join(_venv_lib, _entry)):
                sys.path.insert(0, os.path.join(_venv_lib, _entry, "site-packages"))
                break

    # Make the UDF modules importable, mirroring CPythonUdfBridge.cpp's initPythonOnce.
    sys.path.insert(0, os.environ.get("NES_UDF_PATH", "."))

    _libc = ffi.dlopen(None)
    _lock = threading.Lock()
    _registry = {}
    _next_handle = [1]

    # UdfTypeCode from UdfAbi.h -> C type used to read/write an 8-byte arg/result slot.
    _CTYPE = {
        0: "signed char",         # UDF_BOOL
        1: "signed char",         # UDF_INT8
        2: "short",               # UDF_INT16
        3: "int",                 # UDF_INT32
        4: "long long",           # UDF_INT64
        5: "unsigned char",       # UDF_UINT8
        6: "unsigned short",      # UDF_UINT16
        7: "unsigned int",        # UDF_UINT32
        8: "unsigned long long",  # UDF_UINT64
        9: "float",               # UDF_FLOAT32
        10: "double",             # UDF_FLOAT64
    }
    _VARSIZED = 11

    def _malloc_cstr(data: bytes):
        buf = ffi.cast("char *", _libc.malloc(len(data) + 1))
        ffi.buffer(buf, len(data) + 1)[:] = data + b"\\x00"
        return buf

    def _set_error(errormessage, message: str):
        if errormessage:
            errormessage[0] = _malloc_cstr(message.encode())

    def _to_py_arg(value_ptr, length, is_null, type_code):
        if is_null:
            return None
        if type_code == _VARSIZED:
            return bytes(ffi.buffer(value_ptr, length))
        return ffi.cast(_CTYPE[type_code] + " *", value_ptr)[0]

    def _write_result(value, return_type, result_scalar, result_string, result_string_len, result_null):
        if value is None:
            result_null[0] = 1
            return
        if return_type == _VARSIZED:
            if isinstance(value, str):
                value = value.encode()
            if not isinstance(value, (bytes, bytearray)):
                raise TypeError("VARSIZED-returning UDF must return bytes or str")
            buf = ffi.cast("char *", _libc.malloc(len(value) if len(value) > 0 else 1))
            if len(value) > 0:
                ffi.buffer(buf, len(value))[:] = bytes(value)
            result_string[0] = buf
            result_string_len[0] = len(value)
            return
        ffi.cast(_CTYPE[return_type] + " *", result_scalar)[0] = value

    @ffi.def_extern()
    def initialize_udf(entrypoint, argc, arg_type_codes, return_type_code, errormessage):
        full = ffi.string(entrypoint).decode()
        module_name, _, func_name = full.rpartition(".")
        if not module_name:
            _set_error(errormessage, "entrypoint must be of the form 'module.function'")
            return -1
        try:
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
            if not callable(func):
                raise TypeError(f"'{full}' is not callable")
        except Exception as exc:  # noqa: BLE001 -- reported to the caller, not re-raised
            _set_error(errormessage, f"{type(exc).__name__}: {exc}")
            return -1

        with _lock:
            handle = _next_handle[0]
            _next_handle[0] += 1
            _registry[handle] = (func, [arg_type_codes[i] for i in range(argc)], return_type_code)
        return handle

    @ffi.def_extern()
    def execute_udf_row(handle, arg_values, arg_lens, arg_nulls, result_scalar,
                         result_string, result_string_len, result_null, errormessage):
        result_null[0] = 0
        with _lock:
            entry = _registry.get(handle)
        if entry is None:
            _set_error(errormessage, "invalid UDF handle")
            return 0  # UDF_ERROR

        func, arg_types, return_type = entry
        args = [
            _to_py_arg(arg_values[i], arg_lens[i], arg_nulls[i], arg_types[i])
            for i in range(len(arg_types))
        ]
        try:
            result = func(*args)
            _write_result(result, return_type, result_scalar, result_string, result_string_len, result_null)
        except Exception as exc:  # noqa: BLE001 -- reported to the caller, not re-raised
            _set_error(errormessage, f"{type(exc).__name__}: {exc}")
            return 0  # UDF_ERROR
        return 1  # UDF_OK

    @ffi.def_extern()
    def cleanup_udf(handle):
        with _lock:
            _registry.pop(handle, None)
""")

output_path = sys.argv[1] if len(sys.argv) > 1 else "nes_pypy_udf_bridge.c"
ffibuilder.emit_c_code(output_path)
