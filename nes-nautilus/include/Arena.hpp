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
#include <memory>
#include <span>
#include <utility>
#include <vector>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
class VariableSizedData;

/// The arena is a memory management system that provides memory to the operators during a pipeline invocation.
/// As the memory is destroyed / returned to the arena after the pipeline invocation, the memory is not persistent and thus, it is not
/// suitable for storing state across pipeline invocations. For storing state across pipeline invocations, the operator handler should be used.
struct Arena
{
    explicit Arena(std::shared_ptr<AbstractBufferProvider> bufferProvider) : bufferProvider(std::move(bufferProvider)) { }

    /// Allocating memory by the buffer provider. There are three cases:
    /// 1. The required size is larger than the buffer provider's buffer size. In this case, we allocate an unpooled buffer.
    /// 2. The required size is larger than the last buffer size. In this case, we allocate a new buffer of fixed size.
    /// 3. The required size is smaller than the last buffer size. In this case, we return the pointer to the address in the last buffer.
    std::span<std::byte> allocateMemory(size_t sizeInBytes);

    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::vector<TupleBuffer> fixedSizeBuffers;
    std::vector<TupleBuffer> unpooledBuffers;
    size_t lastAllocationSize{0};
    size_t currentOffset{0};
};

/// Nautilus Wrapper for the Arena
struct ArenaRef
{
    explicit ArenaRef(const nautilus::val<Arena*>& arenaRef) : arenaRef(arenaRef), availableSpaceForPointer(0), spacePointer(nullptr) { }

    /// Allocates memory from the arena. If the available space for the pointer is smaller than the required size, we allocate a new buffer from the arena.
    nautilus::val<int8_t*> allocateMemory(const nautilus::val<size_t>& sizeInBytes);

    VariableSizedData allocateVariableSizedData(const nautilus::val<uint64_t>& sizeInBytes);

    [[nodiscard]] nautilus::val<Arena*> getArena() const;

private:
    nautilus::val<Arena*> arenaRef;
    nautilus::val<size_t> availableSpaceForPointer;
    nautilus::val<int8_t*> spacePointer;
};
}
