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

}
