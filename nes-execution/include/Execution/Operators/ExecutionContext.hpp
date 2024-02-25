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
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <cstdint>
#include <memory>
#include <unordered_map>

namespace NES::Runtime::Execution {
using namespace Nautilus;
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
    ExecutionContext(const Value<MemRef>& workerContext, const Value<MemRef>& pipelineContext);

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
    Value<MemRef> getGlobalOperatorHandler(uint64_t handlerIndex);

    /**
     * @brief Get worker id of the current execution.
     * @return Value<UInt64>
     */
    Value<UInt64> getWorkerId();

    /**
     * @brief Allocate a new tuple buffer.
     * @return Value<MemRef>
     */
    Value<MemRef> allocateBuffer();

    /**
     * @brief Emit a record buffer to the next pipeline or sink.
     * @param record buffer.
     */
    void emitBuffer(const RecordBuffer& rb);

    /**
     * @brief Returns the pipeline context
     * @return Value<MemRef> to the pipeline context
     */
    const Value<MemRef>& getPipelineContext() const;

    /**
     * @brief Returns the worker context
     * @return Value<MemRef> to the worker context
     */
    const Value<MemRef>& getWorkerContext() const;

    /**
     * @brief Returns the current origin id. This is set in the scan.
     * @return Value<UInt64> origin id
     */
    const Value<UInt64>& getOriginId() const;

    /**
     * @brief Sets the current origin id.
     * @param origin
     */
    void setOrigin(Value<UInt64> origin);

    /**
     * @brief Returns the current watermark ts. This is set in the scan.
     * @return Value<UInt64> watermark ts
     */
    const Value<UInt64>& getWatermarkTs() const;

    /**
     * @brief Sets the current valid watermark ts.
     * @param watermarkTs
     */
    void setWatermarkTs(Value<UInt64> watermarkTs);

    /**
     * @brief Sets the current sequence number
     * @param sequenceNumber
     */
    void setSequenceNumber(Value<UInt64> sequenceNumber);

    /**
     * @brief Returns current sequence number
     * @return Value<UInt64> sequence number
     */
    const Value<UInt64>& getSequenceNumber() const;

    /**
     * @brief Returns current chunk number
     * @return Value<UInt64> chunk number
     */
    const Value<UInt64>& getChunkNumber() const;

    /**
     * @brief Sets the current chunk number
     * @param chunkNumber
     */
    void setChunkNumber(Value<UInt64> chunkNumber);

    /**
     * @brief Returns last chunk
     * @return Value<Boolean>&
     */
    const Value<Boolean>& getLastChunk() const;

    /**
     * @brief Removes the sequence state for the current <OrigindId, SequenceNumber>
     */
    void removeSequenceState() const;

    /**
     * @brief Checks if all chunks have been seen
     * @return True or false
     */
    Value<Boolean> isLastChunk() const;

    /**
     * @brief Gets the next chunk number for the emitted tuple buffers
     * @return Value<UInt64>
     */
    Value<UInt64> getNextChunkNr() const;

    /**
     * @brief Sets last chunk
     * @param isLastChunk
     */
    void setLastChunk(Value<Boolean> isLastChunk);

    /**
     * @brief Returns the current time stamp ts. This is set by a time function
     * @return Value<UInt64> timestamp ts
     */
    const Value<UInt64>& getCurrentTs() const;

    /**
     * @brief Sets the current processing timestamp.
     * @param ts
     */
    void setCurrentTs(Value<UInt64> ts);

  private:
    std::unordered_map<const Operators::Operator*, std::unique_ptr<Operators::OperatorState>> localStateMap;
    Value<MemRef> workerContext;
    Value<MemRef> pipelineContext;
    Value<UInt64> origin;
    Value<UInt64> watermarkTs;
    Value<UInt64> currentTs;
    Value<UInt64> sequenceNumber;
    Value<UInt64> chunkNumber;
    Value<Boolean> lastChunk;
};

}// namespace NES::Runtime::Execution

#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_EXECUTIONCONTEXT_HPP_
