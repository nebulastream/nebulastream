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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHDISTINCT_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHDISTINCT_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <unordered_set>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Batch distinct operator.
 * The batch distinct operator, consumes input tuples and materializes them in a global state, a hash set.
 * If all input records are processed, we return the hash set that contains.
 */
class BatchDistinct : public ExecutableOperator {
  public:
    /**
     * @brief Construct a new BatchDistinct operator
     *
     * @param operatorHandlerIndex operator handler index
     * @param dataTypes data types of the input tuples
     */
    BatchDistinct(const uint64_t operatorHandlerIndex, const std::vector<PhysicalTypePtr>& dataTypes);

    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void setup(ExecutionContext& executionCtx) const override;


  private:
    const std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider;
    const uint64_t operatorHandlerIndex;
    const std::vector<PhysicalTypePtr> dataTypes;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHDISTINCT_HPP_
