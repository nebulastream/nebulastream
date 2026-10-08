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
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// What `appendTuples` needs to know about a memory layout (@see TupleBufferRef::getBufferLayout).
struct BufferLayout
{
    /// Bytes that repeat per tuple: those of tuple `i` start at `offset + i * stride`.
    struct Strided
    {
        uint64_t offset = 0;
        uint64_t stride = 0;
    };

    /// The tuple data, `stride` bytes per tuple: one entry for a row layout, one per column for a columnar layout.
    std::vector<Strided> segments;
    /// The variable sized references, one entry per variable sized field.
    std::vector<Strided> references;
    uint64_t capacity = 0;
    uint64_t bufferSize = 0;
};

/// Appends the tuples of `source` behind those of `target`, rewrites the copied variable sized references and updates the tuple count
/// of `target`. A payload in a single source child is appended to the target's last child if it fits; otherwise the source's children
/// are attached to `target`.
/// The tuples must fit into `target`, and both buffers must hold at least `layout.bufferSize` bytes.
/// No buffer involved, including any other buffer sharing the target's last child, may be written concurrently.
void appendTuples(TupleBuffer& target, const TupleBuffer& source, const BufferLayout& layout);

}
