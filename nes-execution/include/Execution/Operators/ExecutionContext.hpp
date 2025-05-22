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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Execution/Operators/Operator.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/val_concepts.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>
#include <val.hpp>

namespace NES::Runtime::Execution
{
using namespace Nautilus;

/// The arena is a memory management system that provides memory to the operators during a pipeline invocation.
/// As the memory is destroyed / returned to the arena after the pipeline invocation, the memory is not persistent and thus, it is not
/// suitable for storing state across pipeline invocations. For storing state across pipeline invocations, the operator handler should be used.
struct Arena
{
    explicit Arena(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider) : bufferProvider(std::move(bufferProvider)) { }

    /// Allocating memory by the buffer provider. There are three cases:
    /// 1. The required size is larger than the buffer provider's buffer size. In this case, we allocate an unpooled buffer.
    /// 2. The required size is larger than the last buffer size. In this case, we allocate a new buffer of fixed size.
    /// 3. The required size is smaller than the last buffer size. In this case, we return the pointer to the address in the last buffer.
    int8_t* allocateMemory(const size_t sizeInBytes)
    {
        /// Case 1
        if (bufferProvider->getBufferSize() < sizeInBytes)
        {
            const auto unpooledBufferOpt = bufferProvider->getUnpooledBuffer(sizeInBytes);
            if (not unpooledBufferOpt.has_value())
            {
                throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size " + std::to_string(sizeInBytes));
            }
            unpooledBuffers.emplace_back(unpooledBufferOpt.value());
            lastAllocationSize = sizeInBytes;
            return unpooledBuffers.back().getBuffer();
        }


        if (fixedSizeBuffers.empty())
        {
            fixedSizeBuffers.emplace_back(bufferProvider->getBufferBlocking());
            lastAllocationSize = bufferProvider->getBufferSize();
            return fixedSizeBuffers.back().getBuffer();
        }

        /// Case 2
        if (lastAllocationSize < currentOffset + sizeInBytes)
        {
            fixedSizeBuffers.emplace_back(bufferProvider->getBufferBlocking());
        }

        /// Case 3
        auto& lastBuffer = fixedSizeBuffers.back();
        lastAllocationSize = lastBuffer.getBufferSize();
        auto* const result = lastBuffer.getBuffer() + currentOffset;
        currentOffset += sizeInBytes;
        return result;
    }

    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::vector<Memory::TupleBuffer> fixedSizeBuffers;
    std::vector<Memory::TupleBuffer> unpooledBuffers;
    size_t lastAllocationSize{0};
    size_t currentOffset{0};
};

/// Nautilus Wrapper for the Arena
struct ArenaRef
{
    explicit ArenaRef(const nautilus::val<Arena*>& arenaRef) : arenaRef(arenaRef), availableSpaceForPointer(0), spacePointer(nullptr) { }

    /// Allocates memory from the arena. If the available space for the pointer is smaller than the required size, we allocate a new buffer from the arena.
    nautilus::val<int8_t*> allocateMemory(const nautilus::val<size_t>& sizeInBytes)
    {
        /// If the available space for the pointer is smaller than the required size, we allocate a new buffer from the arena.
        /// We use the arena's allocateMemory function to allocate a new buffer and set the available space for the pointer to the last allocation size.
        /// Further, we set the space pointer to the beginning of the new buffer.
        if (availableSpaceForPointer < sizeInBytes)
        {
            spacePointer = nautilus::invoke(
                +[](Arena* arena, const size_t sizeInBytesVal) -> int8_t* { return arena->allocateMemory(sizeInBytesVal); },
                arenaRef,
                sizeInBytes);
            availableSpaceForPointer
                = Nautilus::Util::readValueFromMemRef<size_t>(Nautilus::Util::getMemberRef(arenaRef, &Arena::lastAllocationSize));
        }
        availableSpaceForPointer -= sizeInBytes;
        auto result = spacePointer;
        spacePointer += sizeInBytes;
        return result;
    }

private:
    nautilus::val<Arena*> arenaRef;
    nautilus::val<size_t> availableSpaceForPointer;
    nautilus::val<int8_t*> spacePointer;
};

/// Struct that combines the arena and the buffer provider. This struct combines the functionality of the arena and the buffer provider,
/// allowing the operator to allocate two different types of memory, in regard to their lifetime.
/// 1. Memory for a pipeline invocation: Arena
/// 2. Memory for a query: bufferProvider
struct PipelineMemoryProvider
{
    explicit PipelineMemoryProvider(
        const nautilus::val<Arena*>& arena, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
        : arena(arena), bufferProvider(bufferProvider)
    {
    }

    explicit PipelineMemoryProvider(ArenaRef arena, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
        : arena(std::move(arena)), bufferProvider(bufferProvider)
    {
    }

    ArenaRef arena;
    nautilus::val<Memory::AbstractBufferProvider*> bufferProvider;
};

/// The execution context provides access to functionality, such as emitting a record buffer to the next pipeline or sink as well
/// as access to operator states from the nautilus-runtime.
/// We differentiate between local and global operator state.
/// Local operator state lives throughout one pipeline invocation, i.e., over one tuple buffer. It gets initialized in the open call and cleared in the close call.
/// An example is to store the pointer to the current window in a local state so that the window can be accessed when processing the next tuple.
/// Global operator state lives throughout the whole existence of the pipeline. It gets initialized in the setup call and cleared in the terminate call.
/// As the pipeline exists as long as the query exists, the global operator state exists as long as the query runs.
/// An example is to store the windows of a window operator in the global state so that the windows can be accessed in the next pipeline invocation.
struct ExecutionContext final
{
    explicit ExecutionContext(const nautilus::val<PipelineExecutionContext*>& pipelineContext, const nautilus::val<Arena*>& arena);

    void setLocalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state);
    Operators::OperatorState* getLocalState(const Operators::Operator* op);

    [[nodiscard]] nautilus::val<OperatorHandler*> getGlobalOperatorHandler(uint64_t handlerIndex) const;
    [[nodiscard]] nautilus::val<WorkerThreadId> getWorkerThreadId() const;
    /// Use allocateBuffer if you want to allocate space that lives for multiple pipeline invocations, i.e., query lifetime.
    /// You must take care of the memory management yourself, i.e., when/how should the tuple buffer be returned to the buffer provider.
    [[nodiscard]] nautilus::val<Memory::TupleBuffer*> allocateBuffer() const;

    /// Use allocateMemory if you want to allocate memory that lives for one pipeline invocation, i.e., tuple buffer lifetime.
    /// You do not have to take care of the memory management yourself, as the memory is automatically destroyed after the pipeline invocation.
    [[nodiscard]] nautilus::val<int8_t*> allocateMemory(const nautilus::val<size_t>& sizeInBytes);


    /// Emit a record buffer to the successor pipeline(s) or sink(s)
    void emitBuffer(const RecordBuffer& buffer) const;

    std::unordered_map<const Operators::Operator*, std::unique_ptr<Operators::OperatorState>> localStateMap;
    nautilus::val<PipelineExecutionContext*> pipelineContext;
    PipelineMemoryProvider pipelineMemoryProvider;
    nautilus::val<OriginId> originId; /// Stores the current origin id of the incoming tuple buffer. This is set in the scan.
    nautilus::val<Timestamp> watermarkTs; /// Stores the watermark timestamp of the incoming tuple buffer. This is set in the scan.
    nautilus::val<Timestamp> currentTs; /// Stores the current time stamp. This is set by a time function
    nautilus::val<SequenceNumber> sequenceNumber; /// Stores the sequence number id of the incoming tuple buffer. This is set in the scan.
    nautilus::val<ChunkNumber> chunkNumber; /// Stores the chunk number of the incoming tuple buffer. This is set in the scan.
    nautilus::val<bool> lastChunk;
};

}
