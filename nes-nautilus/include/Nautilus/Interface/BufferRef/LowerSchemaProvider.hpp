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

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>

namespace NES
{

/// Enum to identify the memory layout in which we want to represent the schema physically.
enum class MemoryLayoutType : uint8_t
{
    ROW_LAYOUT = 0,
    COLUMNAR_LAYOUT = 1
};

/// Lowers a schema and its memory layout type to a specific TupleBufferRef
class LowerSchemaProvider
{
public:
    static std::shared_ptr<TupleBufferRef> lowerSchema(uint64_t bufferSize, const Schema& schema, MemoryLayoutType layoutType);
};

}
