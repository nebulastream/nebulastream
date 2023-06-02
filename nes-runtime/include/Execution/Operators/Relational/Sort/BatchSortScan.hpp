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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTSCAN_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTSCAN_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief BatchSortScan operator that sorts a batch of records stored in the BatchSortOperatorHandler state.
 */
class BatchSortScan : public Operator {
  public:
    /**
     * @brief Construct a new BatchSortScan operator
     * @param operatorHandlerIndex operator handler index
     * @param fieldIdentifiers field identifiers of the records
     * @param dataTypes data types of the records
     */
    BatchSortScan(const uint64_t operatorHandlerIndex,
                  const std::vector<Record::RecordFieldIdentifier>& fieldIdentifiers,
                  const std::vector<PhysicalTypePtr>& dataTypes)
        : operatorHandlerIndex(operatorHandlerIndex), fieldIdentifiers(fieldIdentifiers), dataTypes(dataTypes) {}

    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const std::vector<Record::RecordFieldIdentifier> fieldIdentifiers;
    const std::vector<PhysicalTypePtr> dataTypes;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTSCAN_HPP_
