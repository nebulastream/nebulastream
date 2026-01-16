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
#include <memory>

#include <DataTypes/Schema.hpp>
#include <TupleBufferRef.hpp>
#include <TupleBufferRefDescriptor.hpp>

namespace NES
{

/// Enum to identify the memory layout in which we want to represent the schema physically.
// Todo:
// 1. keep MemoryLayoutType, model string to registry as arg in Descriptor
// 2. replace MemoryLayoutType with string that is key to registry
// -----------------------
// - get rid of enum and replace with strings (for registry)
// - or: generate enum (via code generation, would be extension for registry template)
//      - weird hybrid with switch cases on mandatory enums, and all plugin-extended-enums would need to be handled in 'default' case
// Approach:
// - check how much we depend on the enum
//      - especially in 'DecideMemoryLayout' <-- looks easily replacable with string
//  -> generally, there does not seem to be a strong dependency on the specific 'enum' so removing it and relying on a string might be fine
enum class MemoryLayoutType : uint8_t
{
    ROW_LAYOUT = 0,
    COLUMNAR_LAYOUT = 1,
    INPUT_FORMATTER_LAYOUT = 2
};

/// Lowers a schema and its memory layout type to a specific TupleBufferRef
class LowerSchemaProvider
{
public:
    static std::shared_ptr<TupleBufferRef> lowerSchema(uint64_t bufferSize, const Schema& schema, MemoryLayoutType layoutType, TupleBufferRefDescriptor descriptor);
};

}
