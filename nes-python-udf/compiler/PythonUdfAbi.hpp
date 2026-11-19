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

}
