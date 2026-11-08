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

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nanoarrow/nanoarrow.hpp>

namespace NES
{

/// The Arrow schema of the record batches of `layout`: a struct with one child per field, VARSIZED as binary.
/// @throws CannotLoadUdf for a type Arrow batches of NES do not hold
[[nodiscard]] nanoarrow::UniqueSchema makeArrowSchema(const ArrowBufferRef& layout);

/// Exports the record batch in a finalized `buffer` through the Arrow C Data Interface without copying it: the buffers of the array
/// point into `buffer` and its child buffers, which the array keeps alive until it is released.
[[nodiscard]] nanoarrow::UniqueArray exportArrowArray(const TupleBuffer& buffer, const ArrowBufferRef& layout, const ArrowSchema& schema);

/// Points the header of `buffer` at the columns of `batch`, a struct of `layout`'s schema whose buffers all lie in one body starting
/// at `body`. The body is stored in `buffer` at `bodyLocation`, which is where the header entries point to.
/// @throws UdfExecutionError if `batch` cannot be read through `layout`
void importArrowArrayView(
    const ArrowArrayView& batch,
    const std::byte* body,
    ArrowBufferRef::Location bodyLocation,
    TupleBuffer& buffer,
    const ArrowBufferRef& layout);

/// The size of a raw buffer in a body, padded so that the next one starts 8-byte aligned, see UdfProcessProtocol::RAW_BUFFERS.
[[nodiscard]] constexpr uint64_t paddedSize(const uint64_t size)
{
    return (size + 7) & ~uint64_t{7};
}

/// An Arrow buffer of a record batch: the field and region it belongs to, its bytes, and where the header of its TupleBuffer says it is.
struct ArrowRegion
{
    size_t field;
    ArrowBufferRef::Region region;
    std::span<const std::byte> bytes;
    ArrowBufferRef::Location location;
};

/// The Arrow buffers of the record batch in a finalized `buffer`, field by field in the order of `layout`, as a raw body lists them,
/// read off its header. They point into `buffer` and its child buffers; a NULL buffer is empty.
[[nodiscard]] std::vector<ArrowRegion> arrowRegionsOf(const TupleBuffer& buffer, const ArrowBufferRef& layout);
[[nodiscard]] std::vector<std::span<const std::byte>> arrowBuffersOf(const TupleBuffer& buffer, const ArrowBufferRef& layout);

/// Validates `rows` rows whose Arrow buffers, as arrowRegionsOf orders them, are `buffers`, as a raw body would be validated.
/// @throws UdfExecutionError if they are not a valid batch of `layout`'s schema
void validateArrowBuffers(
    uint64_t rows, std::span<const std::span<const std::byte>> buffers, const ArrowBufferRef& layout, const ArrowSchema& schema);

/// The size of the raw body of `buffers`, each framed as [size][bytes, padded].
[[nodiscard]] uint64_t rawBodySize(std::span<const std::span<const std::byte>> buffers);

/// Validates a raw body of `rows` rows, as a process of UdfProcessProtocol::RAW_BUFFERS answers, and points the header of `buffer`
/// at its columns. The body is stored in `buffer` at `bodyLocation`.
/// @throws UdfExecutionError if the body is not a valid batch of `layout`'s schema
void importRawBody(
    const std::byte* body,
    uint64_t bodySize,
    uint64_t rows,
    ArrowBufferRef::Location bodyLocation,
    TupleBuffer& buffer,
    const ArrowBufferRef& layout,
    const ArrowSchema& schema);

}
