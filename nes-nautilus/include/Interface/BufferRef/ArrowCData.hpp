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
#include <vector>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

/// ABI of the Arrow C Data Interface, copied verbatim as the specification asks for:
/// https://arrow.apache.org/docs/format/CDataInterface.html
/// pyarrow consumes it via RecordBatch._import_from_c and produces it via RecordBatch._export_to_c.
#ifndef ARROW_C_DATA_INTERFACE
    #define ARROW_C_DATA_INTERFACE
    #define ARROW_FLAG_DICTIONARY_ORDERED 1
    #define ARROW_FLAG_NULLABLE 2
    #define ARROW_FLAG_MAP_KEYS_SORTED 4

/// NOLINTBEGIN
struct ArrowSchema
{
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray
{
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};
/// NOLINTEND
#endif

namespace NES::Arrow
{

/// Describes the buffer as a record batch (a struct array with one child per field) without copying fixed-size columns.
/// The exported array keeps the TupleBuffer alive until it is released. VARSIZED columns spanning multiple child
/// buffers are the only copies, because Arrow requires the payload of a column to be contiguous.
/// The caller owns both structs and has to call their release callbacks. They can be released from any thread.
void exportBatch(const TupleBuffer& buffer, const ArrowBufferRef& layout, ArrowArray& outArray, ArrowSchema& outSchema);

/// Copies a record batch into TupleBuffers with the given layout. Returns at least one buffer, which is empty for an empty
/// batch, and more than one if the batch exceeds the layout's capacity. The columns are matched by position.
/// Does not take ownership of array and schema.
/// @throws InvalidArrowData if the batch does not match the layout, e.g. other types or nulls in a non-nullable field
[[nodiscard]] std::vector<TupleBuffer>
importBatch(const ArrowArray& array, const ArrowSchema& schema, const ArrowBufferRef& layout, AbstractBufferProvider& bufferProvider);

}
