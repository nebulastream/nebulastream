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
#include <Nautilus/DataTypes/VarVal.hpp>
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
    ExecutionContext(const Nautilus::MemRefVal& workerContext, const Nautilus::MemRefVal& pipelineContext);

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
    Nautilus::MemRefVal getGlobalOperatorHandler(uint64_t handlerIndex);

    /**
     * @brief Get worker thread id of the current execution.
     * @return UInt32
     */
    Nautilus::UInt32Val getWorkerThreadId();

    /**
     * @brief Allocate a new tuple buffer.
     * @return Nautilus::MemRefVal
     */
    Nautilus::MemRefVal allocateBuffer();

    /**
     * @brief Emit a record buffer to the next pipeline or sink.
     * @param record buffer.
     */
    void emitBuffer(const RecordBuffer& rb);

    /**
     * @brief Returns the pipeline context
     * @return Nautilus::MemRefVal to the pipeline context
     */
    const Nautilus::MemRefVal& getPipelineContext() const;

    /**
     * @brief Returns the worker context
     * @return Nautilus::MemRefVal to the worker context
     */
    const Nautilus::MemRefVal& getWorkerContext() const;

    /**
     * @brief Returns the current origin id. This is set in the scan.
     * @return Nautilus::UInt64Val origin id
     */
    const Nautilus::UInt64Val& getOriginId() const;

    /**
     * @brief Returns the current statistic id. This is set in the Operator::open()
     * @return Nautilus::UInt64Val statistic id
     */
    const Nautilus::UInt64Val& getCurrentStatisticId() const;

    /**
     * @brief Sets the current origin id.
     * @param origin
     */
    void setOrigin(Nautilus::UInt64Val origin);

    /**
     * @brief Sets the current statistic id of the current tuple buffer in this execution context
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     */
    void setCurrentStatisticId(Nautilus::UInt64Val statisticId);

    /**
     * @brief Returns the current watermark ts. This is set in the scan.
     * @return Nautilus::UInt64Val watermark ts
     */
    const Nautilus::UInt64Val& getWatermarkTs() const;

    /**
     * @brief Sets the current valid watermark ts.
     * @param watermarkTs
     */
    void setWatermarkTs(const Nautilus::UInt64Val& watermarkTs);

    /**
     * @brief Sets the current sequence number
     * @param sequenceNumber
     */
    void setSequenceNumber(Nautilus::UInt64Val sequenceNumber);

    /**
     * @brief Returns current sequence number
     * @return Nautilus::UInt64Val sequence number
     */
    const Nautilus::UInt64Val& getSequenceNumber() const;

    /**
     * @brief Returns current chunk number
     * @return Nautilus::UInt64Val chunk number
     */
    const Nautilus::UInt64Val& getChunkNumber() const;

    /**
     * @brief Sets the current chunk number
     * @param chunkNumber
     */
    void setChunkNumber(Nautilus::UInt64Val chunkNumber);

    /**
     * @brief Returns last chunk
     * @return Nautilus::BooleanVal&
     */
    const Nautilus::BooleanVal& getLastChunk() const;

    /**
     * @brief Removes the sequence state for the current <OrigindId, SequenceNumber>
     */
    void removeSequenceState() const;

    /**
     * @brief Checks if all chunks have been seen
     * @return True or false
     */
    Nautilus::BooleanVal isLastChunk() const;

    /**
     * @brief Gets the next chunk number for the emitted tuple buffers
     * @return Nautilus::UInt64Val
     */
    Nautilus::UInt64Val getNextChunkNr() const;

    /**
     * @brief Sets last chunk
     * @param isLastChunk
     */
    void setLastChunk(Nautilus::BooleanVal isLastChunk);

    /**
     * @brief Returns the current time stamp ts. This is set by a time function
     * @return Nautilus::UInt64Val timestamp ts
     */
    const Nautilus::UInt64Val& getCurrentTs() const;

    /**
     * @brief Sets the current processing timestamp.
     * @param ts
     */
    void setCurrentTs(const Nautilus::UInt64Val& ts);

  private:
    std::unordered_map<const Operators::Operator*, std::unique_ptr<Operators::OperatorState>> localStateMap;
    Nautilus::MemRefVal workerContext;
    Nautilus::MemRefVal pipelineContext;
    Nautilus::UInt64Val origin;
    Nautilus::UInt64Val statisticId;
    Nautilus::UInt64Val watermarkTs;
    Nautilus::UInt64Val currentTs;
    Nautilus::UInt64Val sequenceNumber;
    Nautilus::UInt64Val chunkNumber;
    Nautilus::BooleanVal lastChunk;
};

}// namespace NES::Runtime::Execution

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_EXECUTIONCONTEXT_HPP_
