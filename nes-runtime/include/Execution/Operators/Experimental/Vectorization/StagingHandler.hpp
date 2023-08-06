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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXPERIMENTAL_VECTORIZATION_STAGINGHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXPERIMENTAL_VECTORIZATION_STAGINGHANDLER_HPP_

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
     * @param tupleBuffer the tuple buffer.
     * @param stageBufferCapacity the maximum number of tuples to store in the tuple buffer.
     */
    StagingHandler(std::unique_ptr<TupleBuffer> tupleBuffer, uint64_t stageBufferCapacity);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

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
     * @return Get a raw pointer to the tuple staging buffer.
     */
    TupleBuffer* getTupleBuffer() const;

    /**
     * @return Get the current offset for writing to the staging buffer.
     */
    uint64_t getCurrentWritePosition() const;

    /**
     * @return Increment the current write offset of the staging buffer.
     */
    void incrementWritePosition();

private:
    std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider;
    std::unique_ptr<TupleBuffer> tupleBuffer;
    uint64_t stageBufferCapacity;
    uint64_t currentWritePosition;
};

} // namespace NES::Runtime::Execution::Operators

#endif // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXPERIMENTAL_VECTORIZATION_STAGINGHANDLER_HPP_