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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_STAGINGHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_STAGINGHANDLER_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This handler is designed to handle the materialization of tuples for operators.
 */
class StagingHandler : public OperatorHandler {
public:
    /**
     * @brief Constructor.
     * @param memoryProvider the memory layout that describes the tuple buffer.
     * @param tupleBufferAddress the memory address of the tuple buffer.
     * @param stageBufferCapacity the maximum number of tuples to store in the tuple buffer.
     */
    StagingHandler(std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider, const Value<MemRef>& tupleBufferAddress, uint64_t stageBufferCapacity);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Write a record to the tuple buffer. This method increases the number of tuples as well as the current write position by one.
     * @param record the record to materialize
     */
    void addRecord(Record& record);

    /**
     * @brief Set the current write position to zero.
     */
    void reset();

    /**
     * @brief Return true if the current write position is greater than or equal to the stage buffer capacity.
     * @return bool
     */
    bool full() const;

    /**
     * @brief Get the reference to the tuple buffer.
     * @return Value<MemRef>
     */
    const Value<MemRef>& getTupleBufferReference() const;

private:
    std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider;
    Value<MemRef> tupleBufferAddress;
    uint64_t stageBufferCapacity;
    RecordBuffer recordBuffer;
    Value<UInt64> currentWritePosition;
};

} // namespace NES::Runtime::Execution::Operators

#endif // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_STAGINGHANDLER_HPP_