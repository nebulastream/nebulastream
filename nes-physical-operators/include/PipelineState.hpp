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
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
/// A node in the state tree. Its bytes belong to this operator; child buffers
/// belong to its downstream operators. TupleBuffer retains the complete tree.
class PipelineStateBuilder
{
public:
    struct Header
    {
        uint64_t magic = 0x4e45535354415445; /// "NESSTATE"
        uint64_t byteCount = 0;
    };

    template <typename T>
    requires std::is_trivially_copyable_v<T>
    void append(const T& value)
    {
        const auto* begin = reinterpret_cast<const std::byte*>(&value);
        bytes.insert(bytes.end(), begin, begin + sizeof(T));
    }

    void addChild(TupleBuffer child) { children.push_back(std::move(child)); }

    template <typename T>
    T* own(std::unique_ptr<T> value)
    {
        auto* pointer = value.get();
        retained.emplace_back(std::shared_ptr<T>(std::move(value)));
        return pointer;
    }

    [[nodiscard]] TupleBuffer finish(const std::shared_ptr<AbstractBufferProvider>& provider)
    {
        auto result = provider->getUnpooledBuffer(sizeof(Header) + bytes.size());
        INVARIANT(result.has_value(), "Could not allocate pipeline state buffer");
        auto memory = result->getAvailableMemoryArea();
        const Header header{.byteCount = bytes.size()};
        std::memcpy(memory.data(), &header, sizeof(header));
        if (not bytes.empty())
        {
            std::memcpy(memory.data() + sizeof(header), bytes.data(), bytes.size());
        }
        result->setNumberOfTuples(0);
        for (auto& child : children)
        {
            std::ignore = result->storeChildBuffer(child);
        }
        return std::move(*result);
    }

private:
    std::vector<std::byte> bytes;
    std::vector<TupleBuffer> children;
    std::vector<std::shared_ptr<void>> retained;
};

class PipelineStateReader
{
public:
    explicit PipelineStateReader(const TupleBuffer& buffer) : buffer(buffer)
    {
        INVARIANT(buffer.getBufferSize() >= sizeof(PipelineStateBuilder::Header), "Truncated pipeline state header");
        std::memcpy(&header, buffer.getAvailableMemoryArea().data(), sizeof(header));
        INVARIANT(header.magic == PipelineStateBuilder::Header{}.magic, "Invalid pipeline state header");
        INVARIANT(header.byteCount <= buffer.getBufferSize() - sizeof(header), "Truncated pipeline state payload");
    }

    template <typename T>
    requires std::is_trivially_copyable_v<T>
    [[nodiscard]] T read()
    {
        INVARIANT(sizeof(T) <= header.byteCount - offset, "Truncated pipeline state");
        T value;
        std::memcpy(&value, buffer.getAvailableMemoryArea().data() + sizeof(header) + offset, sizeof(T));
        offset += sizeof(T);
        return value;
    }

    [[nodiscard]] TupleBuffer child(size_t index) const
    {
        INVARIANT(index < buffer.getNumberOfChildBuffers(), "Missing child pipeline state");
        return buffer.loadChildBuffer(ChildBufferIndex(static_cast<uint32_t>(index)));
    }

    [[nodiscard]] size_t childCount() const { return buffer.getNumberOfChildBuffers(); }

    template <typename T>
    T* own(std::unique_ptr<T> value)
    {
        auto* pointer = value.get();
        retained.emplace_back(std::shared_ptr<T>(std::move(value)));
        return pointer;
    }

    void ensureConsumed() const { INVARIANT(offset == header.byteCount, "Unexpected trailing pipeline state"); }

private:
    TupleBuffer buffer;
    PipelineStateBuilder::Header header;
    size_t offset = 0;
    std::vector<std::shared_ptr<void>> retained;
};
}
