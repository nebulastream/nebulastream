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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_PIPELINEEXECUTIONCONTEXT_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_PIPELINEEXECUTIONCONTEXT_HPP_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <vector>
#include <Exceptions/RuntimeException.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/Common.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution
{

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
        DecomposedQueryPlanId queryId,
        Runtime::BufferManagerPtr bufferProvider,
        size_t numberOfWorkerThreads,
        std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunctionHandler,
        std::function<void(TupleBuffer&)>&& emitToQueryManagerFunctionHandler,
        std::vector<OperatorHandlerPtr> operatorHandlers);

    /// Emits a output tuple buffer to the Runtime. Internally we call the emit function which is a callback to the correct handler.
    void emitBuffer(TupleBuffer& tupleBuffer, WorkerContext&);

    /// Dispatch a buffer as a new task to the query manager.
    /// Consequently, a new task is created and the call returns directly.
    void dispatchBuffer(TupleBuffer tupleBuffer);

    std::vector<OperatorHandlerPtr> getOperatorHandlers();

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

    uint64_t getNumberOfWorkerThreads() const;

    Runtime::BufferManagerPtr getBufferManager() const;

    uint64_t getNextChunkNumber(const SeqNumberOriginId seqNumberOriginId);

    /// Checks if this PipelineExecutionContext has seen all chunks for a given sequence number.
    bool isLastChunk(const SeqNumberOriginId seqNumberOriginId, const uint64_t chunkNumber, const bool isLastChunk);

    /// Removes the sequence state in seqNumberOriginIdToChunkStateInput and seqNumberOriginIdToOutputChunkNumber for the seqNumberOriginId
    void removeSequenceState(const SeqNumberOriginId seqNumberOriginId);

private:
    PipelineId pipelineId;
    DecomposedQueryPlanId queryId;

    std::function<void(TupleBuffer&, WorkerContext&)> emitFunctionHandler;

    std::function<void(TupleBuffer&)> emitToQueryManagerFunctionHandler;

    const std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>> operatorHandlers;

    folly::Synchronized<std::map<SeqNumberOriginId, SequenceState>> seqNumberOriginIdToChunkStateInput;
    folly::Synchronized<std::map<SeqNumberOriginId, uint64_t>> seqNumberOriginIdToOutputChunkNumber;

    const Runtime::BufferManagerPtr bufferProvider;
    size_t numberOfWorkerThreads;
};

} /// namespace NES::Runtime::Execution
#endif /// NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_PIPELINEEXECUTIONCONTEXT_HPP_
