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
#include <cstring>
#include <limits>
#include <span>
#include <type_traits>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::detail
{

/// Row layout shared with the V4L2 source: four UINT64 fields and one VARSIZED field.
struct V4L2FrameTuple
{
    uint64_t timestamp;
    uint64_t width;
    uint64_t height;
    uint64_t pixelFormat;
    VariableSizedAccess image;
};

static_assert(std::is_trivially_copyable_v<V4L2FrameTuple>);
static_assert(offsetof(V4L2FrameTuple, timestamp) == 0);
static_assert(sizeof(V4L2FrameTuple) == 4 * sizeof(uint64_t) + sizeof(VariableSizedAccess));

inline V4L2FrameTuple readV4L2Frame(const TupleBuffer& buffer, const size_t index)
{
    if (buffer.getNumberOfTuples() > buffer.getBufferSize() / sizeof(V4L2FrameTuple) || index >= buffer.getNumberOfTuples())
    {
        throw CannotOpenSink("V4L2 sink received an invalid native frame tuple buffer");
    }
    V4L2FrameTuple frame{};
    std::memcpy(&frame, buffer.getAvailableMemoryArea().data() + index * sizeof(V4L2FrameTuple), sizeof(frame));
    if (frame.width == 0 || frame.height == 0 || frame.pixelFormat == 0 || frame.width > std::numeric_limits<uint32_t>::max()
        || frame.height > std::numeric_limits<uint32_t>::max() || frame.pixelFormat > std::numeric_limits<uint32_t>::max())
    {
        throw CannotOpenSink("V4L2 sink requires nonzero WIDTH, HEIGHT, and PIXEL_FORMAT that fit into UINT32");
    }
    return frame;
}

/// The returned view is owned by the parent buffer. Respect both the offset and
/// length: multiple images can share a child buffer after query processing.
inline std::span<const uint8_t> getV4L2Image(const TupleBuffer& buffer, const VariableSizedAccess& image)
{
    if (image.getIndex().getRawValue() >= buffer.getNumberOfChildBuffers())
    {
        throw CannotOpenSink("V4L2 sink IMAGE references a missing child buffer");
    }
    const auto child = buffer.loadChildBuffer(image.getIndex());
    const auto offset = image.getOffset().getRawOffset();
    const auto size = image.getSize().getRawSize();
    if (size == 0 || offset > child.getBufferSize() || size > child.getBufferSize() - offset)
    {
        throw CannotOpenSink("V4L2 sink IMAGE is empty or extends beyond its child buffer");
    }
    return child.getAvailableMemoryArea<uint8_t>().subspan(offset, size);
}

}
