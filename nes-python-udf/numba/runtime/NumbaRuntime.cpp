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

#include <NumbaRuntime.hpp>

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <span>
#include <ErrorHandling.hpp>
#include <PythonUdfAbi.hpp>

/// Declared only to take their addresses: the JIT calls them with the signatures of Numba's headers. The CPython stand-ins have
/// names of their own (NumbaRuntimeHelpers.c); the table gives them the names Numba's code uses.
extern "C" {
void NRT_Allocate();
void NRT_Free();
void NRT_Reallocate();
void NRT_MemInfo_alloc();
void NRT_MemInfo_alloc_aligned();
void NRT_MemInfo_alloc_dtor();
void NRT_MemInfo_alloc_dtor_safe();
void NRT_MemInfo_alloc_safe();
void NRT_MemInfo_alloc_safe_aligned();
void NRT_MemInfo_call_dtor();
void NRT_MemInfo_data();
void NRT_MemInfo_new();
void NRT_MemInfo_new_varsize();
void NRT_MemInfo_new_varsize_dtor();
void NRT_MemInfo_varsize_alloc();
void NRT_MemInfo_varsize_free();
void NRT_MemInfo_varsize_realloc();
void NRT_MemInfo_size();
void NRT_MemInfo_parent();
void NRT_MemInfo_acquire();
void NRT_MemInfo_release();
void NRT_MemInfo_refcount();
void numba_dict_new();
void numba_dict_new_sized();
void numba_dict_set_method_table();
void numba_dict_insert();
void numba_dict_insert_ez();
void numba_dict_lookup();
void numba_dict_delitem();
void numba_dict_popitem();
void numba_dict_free();
void numba_dict_length();
void numba_dict_iter();
void numba_dict_iter_next();
void numba_dict_iter_sizeof();
void numba_dict_resize();
void numba_list_new();
void numba_list_set_method_table();
void numba_list_append();
void numba_list_base_ptr();
void numba_list_free();
void numba_list_size_address();
void numba_list_length();
void numba_list_allocated();
void numba_list_getitem();
void numba_list_setitem();
void numba_list_delitem();
void numba_list_delete_slice();
void numba_list_resize();
void numba_list_is_mutable();
void numba_list_set_is_mutable();
void numba_list_iter();
void numba_list_iter_next();
void numba_list_iter_sizeof();
void numba_gettyperecord();
void numba_get_PyUnicode_ExtendedCase();
void nes_numba_PyErr_SetString();
void nes_numba_PyObject_Hash();
void nes_numba_PyTuple_Pack();
void nes_numba_PyUnicode_FromKindAndData();
void numba_rnd_shuffle();
void numba_rnd_init();
void numba_get_py_random_state();
void numba_get_np_random_state();
void numba_get_internal_random_state();
void NRT_MemSys_init();
extern uint64_t _numba_hashsecret_djbx33a_suffix;
extern uint64_t _numba_hashsecret_siphash_k0;
extern uint64_t _numba_hashsecret_siphash_k1;
extern void* nes_numba_PyExc_RuntimeError;
}

namespace NES
{

std::span<const NumbaRuntimeSymbol> getNumbaRuntimeSymbols()
{
    static std::once_flag initialized;
    std::call_once(initialized, [] { NRT_MemSys_init(); });
    static const NumbaRuntimeSymbol symbols[]{
        {.name = "numba_rnd_shuffle", .address = reinterpret_cast<void*>(&numba_rnd_shuffle)},
        {.name = "numba_rnd_init", .address = reinterpret_cast<void*>(&numba_rnd_init)},
        {.name = "numba_get_py_random_state", .address = reinterpret_cast<void*>(&numba_get_py_random_state)},
        {.name = "numba_get_np_random_state", .address = reinterpret_cast<void*>(&numba_get_np_random_state)},
        {.name = "numba_get_internal_random_state", .address = reinterpret_cast<void*>(&numba_get_internal_random_state)},
        {.name = "NRT_Allocate", .address = reinterpret_cast<void*>(&NRT_Allocate)},
        {.name = "NRT_Free", .address = reinterpret_cast<void*>(&NRT_Free)},
        {.name = "NRT_Reallocate", .address = reinterpret_cast<void*>(&NRT_Reallocate)},
        {.name = "NRT_MemInfo_alloc", .address = reinterpret_cast<void*>(&NRT_MemInfo_alloc)},
        {.name = "NRT_MemInfo_alloc_aligned", .address = reinterpret_cast<void*>(&NRT_MemInfo_alloc_aligned)},
        {.name = "NRT_MemInfo_alloc_dtor", .address = reinterpret_cast<void*>(&NRT_MemInfo_alloc_dtor)},
        {.name = "NRT_MemInfo_alloc_dtor_safe", .address = reinterpret_cast<void*>(&NRT_MemInfo_alloc_dtor_safe)},
        {.name = "NRT_MemInfo_alloc_safe", .address = reinterpret_cast<void*>(&NRT_MemInfo_alloc_safe)},
        {.name = "NRT_MemInfo_alloc_safe_aligned", .address = reinterpret_cast<void*>(&NRT_MemInfo_alloc_safe_aligned)},
        {.name = "NRT_MemInfo_call_dtor", .address = reinterpret_cast<void*>(&NRT_MemInfo_call_dtor)},
        {.name = "NRT_MemInfo_data", .address = reinterpret_cast<void*>(&NRT_MemInfo_data)},
        {.name = "NRT_MemInfo_new", .address = reinterpret_cast<void*>(&NRT_MemInfo_new)},
        {.name = "NRT_MemInfo_new_varsize", .address = reinterpret_cast<void*>(&NRT_MemInfo_new_varsize)},
        {.name = "NRT_MemInfo_new_varsize_dtor", .address = reinterpret_cast<void*>(&NRT_MemInfo_new_varsize_dtor)},
        {.name = "NRT_MemInfo_varsize_alloc", .address = reinterpret_cast<void*>(&NRT_MemInfo_varsize_alloc)},
        {.name = "NRT_MemInfo_varsize_free", .address = reinterpret_cast<void*>(&NRT_MemInfo_varsize_free)},
        {.name = "NRT_MemInfo_varsize_realloc", .address = reinterpret_cast<void*>(&NRT_MemInfo_varsize_realloc)},
        {.name = "NRT_MemInfo_size", .address = reinterpret_cast<void*>(&NRT_MemInfo_size)},
        {.name = "NRT_MemInfo_parent", .address = reinterpret_cast<void*>(&NRT_MemInfo_parent)},
        {.name = "NRT_MemInfo_acquire", .address = reinterpret_cast<void*>(&NRT_MemInfo_acquire)},
        {.name = "NRT_MemInfo_release", .address = reinterpret_cast<void*>(&NRT_MemInfo_release)},
        {.name = "NRT_MemInfo_refcount", .address = reinterpret_cast<void*>(&NRT_MemInfo_refcount)},
        {.name = "numba_dict_new", .address = reinterpret_cast<void*>(&numba_dict_new)},
        {.name = "numba_dict_new_sized", .address = reinterpret_cast<void*>(&numba_dict_new_sized)},
        {.name = "numba_dict_set_method_table", .address = reinterpret_cast<void*>(&numba_dict_set_method_table)},
        {.name = "numba_dict_insert", .address = reinterpret_cast<void*>(&numba_dict_insert)},
        {.name = "numba_dict_insert_ez", .address = reinterpret_cast<void*>(&numba_dict_insert_ez)},
        {.name = "numba_dict_lookup", .address = reinterpret_cast<void*>(&numba_dict_lookup)},
        {.name = "numba_dict_delitem", .address = reinterpret_cast<void*>(&numba_dict_delitem)},
        {.name = "numba_dict_popitem", .address = reinterpret_cast<void*>(&numba_dict_popitem)},
        {.name = "numba_dict_free", .address = reinterpret_cast<void*>(&numba_dict_free)},
        {.name = "numba_dict_length", .address = reinterpret_cast<void*>(&numba_dict_length)},
        {.name = "numba_dict_iter", .address = reinterpret_cast<void*>(&numba_dict_iter)},
        {.name = "numba_dict_iter_next", .address = reinterpret_cast<void*>(&numba_dict_iter_next)},
        {.name = "numba_dict_iter_sizeof", .address = reinterpret_cast<void*>(&numba_dict_iter_sizeof)},
        {.name = "numba_dict_resize", .address = reinterpret_cast<void*>(&numba_dict_resize)},
        {.name = "numba_list_new", .address = reinterpret_cast<void*>(&numba_list_new)},
        {.name = "numba_list_set_method_table", .address = reinterpret_cast<void*>(&numba_list_set_method_table)},
        {.name = "numba_list_append", .address = reinterpret_cast<void*>(&numba_list_append)},
        {.name = "numba_list_base_ptr", .address = reinterpret_cast<void*>(&numba_list_base_ptr)},
        {.name = "numba_list_free", .address = reinterpret_cast<void*>(&numba_list_free)},
        {.name = "numba_list_size_address", .address = reinterpret_cast<void*>(&numba_list_size_address)},
        {.name = "numba_list_length", .address = reinterpret_cast<void*>(&numba_list_length)},
        {.name = "numba_list_allocated", .address = reinterpret_cast<void*>(&numba_list_allocated)},
        {.name = "numba_list_getitem", .address = reinterpret_cast<void*>(&numba_list_getitem)},
        {.name = "numba_list_setitem", .address = reinterpret_cast<void*>(&numba_list_setitem)},
        {.name = "numba_list_delitem", .address = reinterpret_cast<void*>(&numba_list_delitem)},
        {.name = "numba_list_delete_slice", .address = reinterpret_cast<void*>(&numba_list_delete_slice)},
        {.name = "numba_list_resize", .address = reinterpret_cast<void*>(&numba_list_resize)},
        {.name = "numba_list_is_mutable", .address = reinterpret_cast<void*>(&numba_list_is_mutable)},
        {.name = "numba_list_set_is_mutable", .address = reinterpret_cast<void*>(&numba_list_set_is_mutable)},
        {.name = "numba_list_iter", .address = reinterpret_cast<void*>(&numba_list_iter)},
        {.name = "numba_list_iter_next", .address = reinterpret_cast<void*>(&numba_list_iter_next)},
        {.name = "numba_list_iter_sizeof", .address = reinterpret_cast<void*>(&numba_list_iter_sizeof)},
        {.name = "numba_gettyperecord", .address = reinterpret_cast<void*>(&numba_gettyperecord)},
        {.name = "numba_get_PyUnicode_ExtendedCase", .address = reinterpret_cast<void*>(&numba_get_PyUnicode_ExtendedCase)},
        {.name = "PyErr_SetString", .address = reinterpret_cast<void*>(&nes_numba_PyErr_SetString)},
        {.name = "PyObject_Hash", .address = reinterpret_cast<void*>(&nes_numba_PyObject_Hash)},
        {.name = "PyTuple_Pack", .address = reinterpret_cast<void*>(&nes_numba_PyTuple_Pack)},
        {.name = "PyUnicode_FromKindAndData", .address = reinterpret_cast<void*>(&nes_numba_PyUnicode_FromKindAndData)},
        {.name = "_numba_hashsecret_djbx33a_suffix", .address = static_cast<void*>(&_numba_hashsecret_djbx33a_suffix)},
        {.name = "_numba_hashsecret_siphash_k0", .address = static_cast<void*>(&_numba_hashsecret_siphash_k0)},
        {.name = "_numba_hashsecret_siphash_k1", .address = static_cast<void*>(&_numba_hashsecret_siphash_k1)},
        {.name = "PyExc_RuntimeError", .address = static_cast<void*>(&nes_numba_PyExc_RuntimeError)},
    };
    static const bool matches = std::ranges::equal(
        symbols, NumbaRuntimeSymbolNames, [](const NumbaRuntimeSymbol& symbol, std::string_view name) { return symbol.name == name; });
    INVARIANT(matches, "The Numba runtime symbol table differs from NumbaRuntimeSymbolNames");
    return symbols;
}

}
