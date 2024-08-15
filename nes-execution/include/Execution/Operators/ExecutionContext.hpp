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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_EXECUTIONCONTEXT_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_EXECUTIONCONTEXT_HPP_
#include <Execution/Operators/OperatorState.hpp>
#include <Nautilus/DataTypes/AbstractDataType.hpp>
#include <cstdint>
#include <memory>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <unordered_map>

namespace NES::Runtime::Execution {
class RecordBuffer;

namespace Operators {
class Operator;
class OperatorState;
}// namespace Operators

/**
 * The execution context manages state of operators within a pipeline and provides access to some global functionality.
 * We differentiate between local and global operator state.
 * Local operator state lives throughout one pipeline invocation. It gets initialized in the open call and cleared in the close call.
 * Global operator state lives throughout the whole existence of the pipeline. It gets initialized in the setup call and cleared in the terminate call.
 */
class ExecutionContext final {
  public:
    /**
     * @brief Create new execution context with mem refs to the worker context and the pipeline context.
     * @param workerContext reference to the worker context.
     * @param pipelineContext reference to the pipeline context.
     */
    ExecutionContext(const nautilus::val<int8_t*>& workerContext, const nautilus::val<int8_t*>& pipelineContext);

    /**
     * @brief Set local operator state that keeps state in a single pipeline invocation.
     * @param op reference to the operator to identify the state.
     * @param state operator state.
     */
    void setLocalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state);

    /**
     * @brief Get the operator state by the operator reference.
     * @param op operator reference
     * @return operator state.
     */
    Operators::OperatorState* getLocalState(const Operators::Operator* op);

    /**
     * @brief Get the global operator state.
     * @param handlerIndex reference to the operator to identify the state.
     */
    nautilus::val<int8_t*> getGlobalOperatorHandler(uint64_t handlerIndex);

    /**
     * @brief Get worker thread id of the current execution.
     * @return UInt32
     */
    nautilus::val<uint32_t> getWorkerThreadId();

    /**
     * @brief Allocate a new tuple buffer.
     * @return nautilus::val<int8_t*>
     */
    nautilus::val<int8_t*> allocateBuffer();

    /**
     * @brief Emit a record buffer to the next pipeline or sink.
     * @param record buffer.
     */
    void emitBuffer(const RecordBuffer& rb);

    /**
     * @brief Returns the pipeline context
     * @return nautilus::val<int8_t*> to the pipeline context
     */
    const nautilus::val<int8_t*>& getPipelineContext() const;

    /**
     * @brief Returns the worker context
     * @return nautilus::val<int8_t*> to the worker context
     */
    const nautilus::val<int8_t*>& getWorkerContext() const;

    /**
     * @brief Returns the current origin id. This is set in the scan.
     * @return nautilus::val<uint64_t> origin id
     */
    const nautilus::val<uint64_t>& getOriginId() const;

    /**
     * @brief Returns the current statistic id. This is set in the Operator::open()
     * @return nautilus::val<uint64_t> statistic id
     */
    const nautilus::val<uint64_t>& getCurrentStatisticId() const;

    /**
     * @brief Sets the current origin id.
     * @param origin
     */
    void setOrigin(nautilus::val<uint64_t> origin);

    /**
     * @brief Sets the current statistic id of the current tuple buffer in this execution context
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     */
    void setCurrentStatisticId(nautilus::val<uint64_t> statisticId);

    /**
     * @brief Returns the current watermark ts. This is set in the scan.
     * @return nautilus::val<uint64_t> watermark ts
     */
    const nautilus::val<uint64_t>& getWatermarkTs() const;

    /**
     * @brief Sets the current valid watermark ts.
     * @param watermarkTs
     */
    void setWatermarkTs(nautilus::val<uint64_t> watermarkTs);

    /**
     * @brief Sets the current sequence number
     * @param sequenceNumber
     */
    void setSequenceNumber(nautilus::val<uint64_t> sequenceNumber);

    /**
     * @brief Returns current sequence number
     * @return nautilus::val<uint64_t> sequence number
     */
    const nautilus::val<uint64_t>& getSequenceNumber() const;

    /**
     * @brief Returns current chunk number
     * @return nautilus::val<uint64_t> chunk number
     */
    const nautilus::val<uint64_t>& getChunkNumber() const;

    /**
     * @brief Sets the current chunk number
     * @param chunkNumber
     */
    void setChunkNumber(nautilus::val<uint64_t> chunkNumber);

    /**
     * @brief Returns last chunk
     * @return nautilus::val<bool>&
     */
    const nautilus::val<bool>& getLastChunk() const;

    /**
     * @brief Removes the sequence state for the current <OrigindId, SequenceNumber>
     */
    void removeSequenceState() const;

    /**
     * @brief Checks if all chunks have been seen
     * @return True or false
     */
    nautilus::val<bool> isLastChunk() const;

    /**
     * @brief Gets the next chunk number for the emitted tuple buffers
     * @return nautilus::val<uint64_t>
     */
    nautilus::val<uint64_t> getNextChunkNr() const;

    /**
     * @brief Sets last chunk
     * @param isLastChunk
     */
    void setLastChunk(nautilus::val<bool> isLastChunk);

    /**
     * @brief Returns the current time stamp ts. This is set by a time function
     * @return nautilus::val<uint64_t> timestamp ts
     */
    const nautilus::val<uint64_t>& getCurrentTs() const;

    /**
     * @brief Sets the current processing timestamp.
     * @param ts
     */
    void setCurrentTs(nautilus::val<uint64_t> ts);

  private:
    std::unordered_map<const Operators::Operator*, std::unique_ptr<Operators::OperatorState>> localStateMap;
    nautilus::val<int8_t*> workerContext;
    nautilus::val<int8_t*> pipelineContext;
    nautilus::val<uint64_t> origin;
    nautilus::val<uint64_t> statisticId;
    nautilus::val<uint64_t> watermarkTs;
    nautilus::val<uint64_t> currentTs;
    nautilus::val<uint64_t> sequenceNumber;
    nautilus::val<uint64_t> chunkNumber;
    nautilus::val<bool> lastChunk;
};

}// namespace NES::Runtime::Execution

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_EXECUTIONCONTEXT_HPP_
