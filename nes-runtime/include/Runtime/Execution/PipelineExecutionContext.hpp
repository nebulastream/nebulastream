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

#include <Exceptions/RuntimeException.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/Common.hpp>
#include <cstdint>
#include <folly/Synchronized.h>
#include <functional>
#include <map>
#include <memory>
#include <vector>

namespace NES::Runtime::Execution {

/**
 * @brief Stores a sequenceNumber and an OriginId
 */
struct SeqNumberOriginId {
    SequenceNumber sequenceNumber = INVALID_SEQ_NUMBER;
    OriginId originId = INVALID_ORIGIN_ID;

    bool operator<(const SeqNumberOriginId& other) const {
        if (sequenceNumber == other.sequenceNumber) {
            return originId < other.originId;
        }
        return sequenceNumber < other.sequenceNumber;
    }

    [[nodiscard]] std::string toString() const {
        std::ostringstream oss;
        oss << "{ seqNumber = " << sequenceNumber << ", originId = " << originId << "}";
        return oss.str();
    }
};

/**
 * @brief Container for storing information, related to the state of a sequence number
 */
struct SequenceState {
    uint64_t lastChunkNumber = 0;
    uint64_t seenChunks = 0;
};

/**
 * @brief The PipelineExecutionContext is passed to a compiled pipeline and offers basic functionality to interact with the Runtime.
 * Via the context, the compile code is able to allocate new TupleBuffers and to emit tuple buffers to the Runtime.
 */
class PipelineExecutionContext : public std::enable_shared_from_this<PipelineExecutionContext> {

  public:
    /**
     * @brief The PipelineExecutionContext is passed to the compiled pipeline and enables interaction with the NES Runtime.
     * @param emitFunctionHandler an handler to receive the emitted buffers from the pipeline.
     * @param emitToQueryManagerFunctionHandler an handler to receive emitted buffers, which are then dispatched to the query manager.
     * @param operatorHandlers a list of operator handlers managed by the pipeline execution context.
     */
    explicit PipelineExecutionContext(
        PipelineId pipelineId,
        DecomposedQueryId queryId,
        Runtime::BufferManagerPtr bufferProvider,
        size_t numberOfWorkerThreads,
        std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunctionHandler,
        std::function<void(TupleBuffer&)>&& emitToQueryManagerFunctionHandler,
        std::vector<OperatorHandlerPtr> operatorHandlers,
        std::optional<std::function<void(TupleBuffer&, WorkerContext&)>> emitMigrationHandler = nullptr);

    /**
     * @brief Emits a output tuple buffer to the Runtime. Internally we call the emit function which is a callback to the correct handler.
     * @param tupleBuffer the output tuple buffer that is passed to the Runtime
     * @param workerContext the worker context
     */
    void emitBuffer(TupleBuffer& tupleBuffer, WorkerContext&);

    /**
    * @brief Dispatch a buffer as a new task to the query manager.
    * Consequently, a new task is created and the call returns directly.
    * @param outputBuffer the output tuple buffer that is passed to the Runtime
    * @param workerContext the worker context
    */
    void dispatchBuffer(TupleBuffer tupleBuffer);

    /**
     * @brief Emits a output tuple to migration pipeline.
     * @param outputBuffer the output tuple buffer that is passed to the Runtime
     * @param workerContext the worker context
     */
    void migrateBuffer(TupleBuffer& buffer, WorkerContextRef workerContext);

    /**
     * @brief Retrieve all registered operator handlers.
     * @return  std::vector<OperatorHandlerPtr>
     */
    std::vector<OperatorHandlerPtr> getOperatorHandlers();

    /**
     * @brief Retrieves a Operator Handler at a specific index and cast its to an OperatorHandlerType.
     * @tparam OperatorHandlerType
     * @param index of the operator handler.
     * @return
     */
    template<class OperatorHandlerType>
    auto getOperatorHandler(std::size_t index) {
        auto size = operatorHandlers.size();
        if (index >= size) {
            throw Exceptions::RuntimeException("PipelineExecutionContext: operator handler at index " + std::to_string(index)
                                               + " is not registered");
        }
        return std::dynamic_pointer_cast<OperatorHandlerType>(operatorHandlers[index]);
    }

    std::vector<PredecessorExecutablePipeline>& getPredecessors() { return predecessors; }

    void addPredecessor(PredecessorExecutablePipeline pred) { predecessors.push_back(pred); }

    std::string toString() const;

    PipelineId getPipelineID() { return this->pipelineId; }

    /**
     * @brief Returns the number of worker threads
     * @return uint64_t
     */
    uint64_t getNumberOfWorkerThreads() const;

    /**
     * @brief Returns the current buffer manager.
     * @return Runtime::BufferManagerPtr
     */
    Runtime::BufferManagerPtr getBufferManager() const;

    /**
     * @brief Returns the next chunk number belonging to a sequence number for emitting a buffer
     * @param seqNumberOriginId
     * @return uint64_t
     */
    uint64_t getNextChunkNumber(const SeqNumberOriginId seqNumberOriginId);

    /**
     * @brief Checks if this PipelineExecutionContext has seen all chunks for a given sequence number.
     * @param seqNumberOriginId
     * @param chunkNumber
     * @param isLastChunk
     * @return True, if all chunks have been seen, false otherwise
     */
    bool isLastChunk(const SeqNumberOriginId seqNumberOriginId, const uint64_t chunkNumber, const bool isLastChunk);

    /**
     * @brief Removes the sequence state in seqNumberOriginIdToChunkStateInput and seqNumberOriginIdToOutputChunkNumber
     * for the seqNumberOriginId
     * @param seqNumberOriginId
     */
    void removeSequenceState(const SeqNumberOriginId seqNumberOriginId);

  private:
    /**
     * @brief Id of the pipeline
     */
    PipelineId pipelineId;
    /**
     * @brief Id of the local qep that owns the pipeline
     */
    DecomposedQueryId queryId;

    /**
     * @brief The emit function handler to react on an emitted tuple buffer.
     */
    std::function<void(TupleBuffer&, WorkerContext&)> emitFunctionHandler;

    /**
    * @brief The emit function handler to react on an emitted tuple buffer.
    */
    std::function<void(TupleBuffer&)> emitToQueryManagerFunctionHandler;

    /**
     * @brief List of registered operator handlers.
     */
    const std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>> operatorHandlers;

    /**
     * @brief The emit function handler to emit migration buffer.
     */
    std::optional<std::function<void(TupleBuffer&, WorkerContext&)>> emitMigrationHandler;

    folly::Synchronized<std::map<SeqNumberOriginId, SequenceState>> seqNumberOriginIdToChunkStateInput;
    folly::Synchronized<std::map<SeqNumberOriginId, uint64_t>> seqNumberOriginIdToOutputChunkNumber;

    const Runtime::BufferManagerPtr bufferProvider;
    size_t numberOfWorkerThreads;

    std::vector<PredecessorExecutablePipeline> predecessors;
};

}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_PIPELINEEXECUTIONCONTEXT_HPP_
