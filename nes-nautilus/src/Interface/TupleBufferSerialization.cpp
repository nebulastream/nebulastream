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

#include <Interface/TupleBufferSerialization.hpp>

#include <cstdint>
#include <istream>
#include <ostream>
#include <tuple>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
struct BufferNodeHeader
{
    uint64_t bufferSize;
    uint64_t numberOfTuples;
    uint64_t numberOfChildren;
};

void writeNode(std::ostream& out, const TupleBuffer& buffer)
{
    const BufferNodeHeader header{
        .bufferSize = buffer.getBufferSize(),
        .numberOfTuples = buffer.getNumberOfTuples(),
        .numberOfChildren = buffer.getNumberOfChildBuffers()};
    out.write(reinterpret_cast<const char*>(&header), sizeof(header)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    const auto payload = buffer.getAvailableMemoryArea<char>();
    out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    if (!out)
    {
        throw CannotSerialize("Failed to write a tuple buffer of {} bytes", header.bufferSize);
    }
    for (uint32_t childIndex = 0; childIndex < header.numberOfChildren; ++childIndex)
    {
        writeNode(out, buffer.loadChildBuffer(ChildBufferIndex{childIndex}));
    }
}

TupleBuffer readNode(std::istream& in, AbstractBufferProvider& bufferProvider)
{
    BufferNodeHeader header{};
    in.read(reinterpret_cast<char*>(&header), sizeof(header)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    if (!in)
    {
        throw CannotDeserialize("Truncated tuple buffer header");
    }
    auto buffer = bufferProvider.getUnpooledBuffer(header.bufferSize);
    if (!buffer)
    {
        throw CannotDeserialize("Cannot allocate a tuple buffer of {} bytes", header.bufferSize);
    }
    auto payload = buffer->getAvailableMemoryArea<char>();
    in.read(payload.data(), static_cast<std::streamsize>(payload.size()));
    if (!in)
    {
        throw CannotDeserialize("Truncated tuple buffer payload of {} bytes", header.bufferSize);
    }
    buffer->setNumberOfTuples(header.numberOfTuples);
    for (uint64_t childIndex = 0; childIndex < header.numberOfChildren; ++childIndex)
    {
        auto child = readNode(in, bufferProvider);
        std::ignore = buffer->storeChildBuffer(child);
    }
    return std::move(buffer.value());
}
}

void serializeTupleBufferTree(std::ostream& out, const TupleBuffer& buffer)
{
    writeNode(out, buffer);
}

TupleBuffer deserializeTupleBufferTree(std::istream& in, AbstractBufferProvider& bufferProvider)
{
    return readNode(in, bufferProvider);
}

}
