/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <array>
#include <string_view>

namespace NES
{

/// The functions that the NES worker provides to compiled Python UDFs (PythonUdfRuntime.cpp): Codon's runtime ABI as NES
/// implements it, and pure libc functions that Codon's stdlib calls. A compiled UDF may call only these, the native functions
/// of the Codon plugins, and LLVM intrinsics. The frontend checks a UDF against this list when it compiles it, the worker
/// before it runs it; the worker's symbol table must define exactly these names.
inline constexpr std::array<std::string_view, 34> PythonUdfRuntimeSymbolNames{
    "seq_alloc",
    "seq_alloc_atomic",
    "seq_realloc",
    "seq_free",
    "seq_exc_offset",
    "seq_alloc_exc",
    "seq_throw",
    "seq_personality",
    "seq_int_from_str",
    "seq_str_int",
    "seq_str_uint",
    "seq_str_float",
    "seq_float_from_str",
    "seq_stdout",
    "seq_stdin",
    "seq_stderr",
    "seq_lock_new",
    "seq_lock_acquire",
    "seq_lock_release",
    "seq_pid",
    "seq_time",
    "seq_time_monotonic",
    "nes_codon_ensure_module_initialized",
    "tolower",
    "toupper",
    "memchr",
    "bcmp",
    "frexp",
    "isalnum",
    "isalpha",
    "isdigit",
    "islower",
    "isspace",
    "isupper"};


/// The functions and globals that code compiled by Numba (COMPILER NUMBA) may use besides PythonUdfRuntimeSymbolNames
/// (seq_alloc for a VARSIZED result) and LLVM intrinsics: Numba's memory runtime NRT, its typed dict and list, its Unicode
/// tables and hash seeds, and the stand-ins of the CPython functions on the error path of Numba's exceptions with run-time
/// values (nes-python-udf/numba/runtime). The worker's Numba symbol table must define exactly these names.
inline constexpr std::array<std::string_view, 69> NumbaRuntimeSymbolNames{
    "numba_rnd_shuffle",
    "numba_rnd_init",
    "numba_get_py_random_state",
    "numba_get_np_random_state",
    "numba_get_internal_random_state",
    "NRT_Allocate",
    "NRT_Free",
    "NRT_Reallocate",
    "NRT_MemInfo_alloc",
    "NRT_MemInfo_alloc_aligned",
    "NRT_MemInfo_alloc_dtor",
    "NRT_MemInfo_alloc_dtor_safe",
    "NRT_MemInfo_alloc_safe",
    "NRT_MemInfo_alloc_safe_aligned",
    "NRT_MemInfo_call_dtor",
    "NRT_MemInfo_data",
    "NRT_MemInfo_new",
    "NRT_MemInfo_new_varsize",
    "NRT_MemInfo_new_varsize_dtor",
    "NRT_MemInfo_varsize_alloc",
    "NRT_MemInfo_varsize_free",
    "NRT_MemInfo_varsize_realloc",
    "NRT_MemInfo_size",
    "NRT_MemInfo_parent",
    "NRT_MemInfo_acquire",
    "NRT_MemInfo_release",
    "NRT_MemInfo_refcount",
    "numba_dict_new",
    "numba_dict_new_sized",
    "numba_dict_set_method_table",
    "numba_dict_insert",
    "numba_dict_insert_ez",
    "numba_dict_lookup",
    "numba_dict_delitem",
    "numba_dict_popitem",
    "numba_dict_free",
    "numba_dict_length",
    "numba_dict_iter",
    "numba_dict_iter_next",
    "numba_dict_iter_sizeof",
    "numba_dict_resize",
    "numba_list_new",
    "numba_list_set_method_table",
    "numba_list_append",
    "numba_list_base_ptr",
    "numba_list_free",
    "numba_list_size_address",
    "numba_list_length",
    "numba_list_allocated",
    "numba_list_getitem",
    "numba_list_setitem",
    "numba_list_delitem",
    "numba_list_delete_slice",
    "numba_list_resize",
    "numba_list_is_mutable",
    "numba_list_set_is_mutable",
    "numba_list_iter",
    "numba_list_iter_next",
    "numba_list_iter_sizeof",
    "numba_gettyperecord",
    "numba_get_PyUnicode_ExtendedCase",
    "PyErr_SetString",
    "PyObject_Hash",
    "PyTuple_Pack",
    "PyUnicode_FromKindAndData",
    "_numba_hashsecret_djbx33a_suffix",
    "_numba_hashsecret_siphash_k0",
    "_numba_hashsecret_siphash_k1",
    "PyExc_RuntimeError"};

/// The libm functions that code compiled by Numba may call besides NumbaRuntimeSymbolNames: stateless, resolved from the
/// process like the libc functions of the Codon path.
inline constexpr std::array<std::string_view, 44> NumbaProcessFunctions{
    "sqrt",      "cbrt",  "exp",    "exp2",   "expm1", "log",   "log2",  "log10",    "log1p",     "pow",  "fmod",
    "remainder", "sin",   "cos",    "tan",    "asin",  "acos",  "atan",  "atan2",    "sinh",      "cosh", "tanh",
    "asinh",     "acosh", "atanh",  "floor",  "ceil",  "round", "trunc", "rint",     "nearbyint", "fabs", "hypot",
    "erf",       "erfc",  "lgamma", "tgamma", "ldexp", "frexp", "modf",  "copysign", "nextafter", "fmin", "fmax"};
}
