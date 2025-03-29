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
#include <functional>
#include <map>
#include <memory>
#include <Exceptions/RuntimeException.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <folly/Synchronized.h>

namespace NES
{
/// Forward declaration of OperatorHandler, which directly includes PipelineExecutionContext
class OperatorHandler;

/**
 * @brief Stores a sequenceNumber and an OriginId
 */
struct SeqNumberOriginId
{
    SequenceNumber sequenceNumber = INVALID_SEQ_NUMBER;
    OriginId originId = INVALID_ORIGIN_ID;

    bool operator<(const SeqNumberOriginId& other) const
    {
        if (sequenceNumber == other.sequenceNumber)
        {
            return originId < other.originId;
        }
        return sequenceNumber < other.sequenceNumber;
    }

    [[nodiscard]] std::string toString() const
    {
        std::ostringstream oss;
        oss << "{ seqNumber = " << sequenceNumber << ", originId = " << originId << "}";
        return oss.str();
    }
};

/**
 * @brief Container for storing information, related to the state of a sequence number
 */
struct SequenceState
{
    uint64_t lastChunkNumber = 0;
    uint64_t seenChunks = 0;
};

/**
 * @brief The PipelineExecutionContext is passed to a compiled pipeline and offers basic functionality to interact with the Runtime.
 * Via the context, the compile code is able to allocate new TupleBuffers and to emit tuple buffers to the Runtime.
 */
class PipelineExecutionContext : public std::enable_shared_from_this<PipelineExecutionContext>
{
public:
    explicit PipelineExecutionContext(
        PipelineId pipelineId,
        QueryId queryId,
        std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
        size_t numberOfWorkerThreads,
        std::function<void(Memory::TupleBuffer&, WorkerContext&)>&& emitFunctionHandler,
        std::function<void(Memory::TupleBuffer&)>&& emitToQueryManagerFunctionHandler,
        std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers);

    /// Emits a output tuple buffer to the Runtime. Internally we call the emit function which is a callback to the correct handler.
    void emitBuffer(Memory::TupleBuffer& tupleBuffer, WorkerContext&);

    /// Dispatch a buffer as a new task to the query manager.
    /// Consequently, a new task is created and the call returns directly.
    void dispatchBuffer(Memory::TupleBuffer tupleBuffer);

    [[nodiscard]] std::vector<std::shared_ptr<OperatorHandler>> getOperatorHandlers();

    template <class OperatorHandlerType>
    auto getOperatorHandler(std::size_t index)
    {
        auto size = operatorHandlers.size();
        if (index >= size)
        {
            throw Exceptions::RuntimeException(
                "PipelineExecutionContext: operator handler at index " + std::to_string(index) + " is not registered");
        }
        return std::dynamic_pointer_cast<OperatorHandlerType>(operatorHandlers[index]);
    }

    std::string toString() const;

    PipelineId getPipelineID() { return this->pipelineId; }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const;

    [[nodiscard]] std::shared_ptr<Memory::AbstractBufferProvider> getBufferManager() const;

    /// Returns the next chunk number belonging to a sequence number for emitting a buffer
    [[nodiscard]] ChunkNumber::Underlying getNextChunkNumber(SeqNumberOriginId seqNumberOriginId);

    /// Checks if this PipelineExecutionContext has seen all chunks for a given sequence number.
    bool isLastChunk(SeqNumberOriginId seqNumberOriginId, ChunkNumber chunkNumber, bool isLastChunk);

    /// Removes the sequence state in seqNumberOriginIdToChunkStateInput and seqNumberOriginIdToOutputChunkNumber for the seqNumberOriginId
    void removeSequenceState(SeqNumberOriginId seqNumberOriginId);

private:
    PipelineId pipelineId;
    QueryId queryId;

    /// Emit function handlers to react on an emitted tuple buffer
    std::function<void(Memory::TupleBuffer&, WorkerContext&)> emitFunctionHandler;
    std::function<void(Memory::TupleBuffer&)> emitToQueryManagerFunctionHandler;

    const std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers;

    folly::Synchronized<std::map<SeqNumberOriginId, SequenceState>> seqNumberOriginIdToChunkStateInput;
    folly::Synchronized<std::map<SeqNumberOriginId, ChunkNumber::Underlying>> seqNumberOriginIdToOutputChunkNumber;

    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    size_t numberOfWorkerThreads;
};
using PipelineExecutionContextPtr = std::shared_ptr<PipelineExecutionContext>;
}
