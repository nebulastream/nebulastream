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

#include <cstdint>
#include <functional>
#include <utility>
#include <nautilus/val.hpp>

namespace NES
{
class VarVal;
struct DataType;

/// Resolves the (pointer, length) of a var-sized payload (VARSIZED or VECTOR) from the 16-byte slot that references it.
/// What the slot contains depends on the layout the value lives in: a PagedVector page or TupleBuffer stores a
/// VariableSizedAccess (child index, offset, size) that must be resolved through the owning buffer, whereas values that
/// were already materialized (hash map entries, arena allocations, parser output) store the pointer and length inline.
/// Composite values (FixedSizedData, StructData) keep the loader of the layout they were read from, so nested var-sized
/// members are resolved lazily on element access and the stored slot is never rewritten.
using VarSizedLoadFn = std::function<std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>(nautilus::val<int8_t*> slot)>;

/// Loader for slots that hold the pointer (8 bytes) followed by the length (8 bytes) inline.
VarSizedLoadFn inlineVarSizedLoader();

/// Reads the non-nullable value of type stored at address. VARSIZED and VECTOR values are resolved through
/// loadVarSized. FIXEDSIZED and STRUCT values keep the loader for their own nested members. All other types are read directly.
VarVal readNestedVarVal(const nautilus::val<int8_t*>& address, const DataType& type, const VarSizedLoadFn& loadVarSized);

}
