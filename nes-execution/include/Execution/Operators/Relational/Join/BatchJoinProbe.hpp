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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINPROBE_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINPROBE_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief Batch join probe operator.
 * The operator receives input records and uses their key to probe a global hash table.
 */
class BatchJoinProbe : public ExecutableOperator {
  public:
    /**
     * @brief Creates a batch join probe operator.
     * @param operatorHandlerIndex index of the operator handler.
     * @param keyExpressions expressions that extract the key fields from the input record.
     * @param keyDataTypes data types of the key fields.
     * @param probeFieldIdentifiers record identifier of the value field in the probe table
     * @param valueDataTypes data types of the value fields
     * @param hashFunction hash function
     */
    BatchJoinProbe(uint64_t operatorHandlerIndex,
                   const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                   const std::vector<PhysicalTypePtr>& keyDataTypes,
                   const std::vector<Record::RecordFieldIdentifier>& probeFieldIdentifiers,
                   const std::vector<PhysicalTypePtr>& valueDataTypes,
                   std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction);
    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const std::vector<Expressions::ExpressionPtr> keyExpressions;
    const std::vector<PhysicalTypePtr> keyDataTypes;
    const std::vector<Record::RecordFieldIdentifier> probeFieldIdentifiers;
    const std::vector<PhysicalTypePtr> valueDataTypes;
    const std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction;
    uint64_t keySize;
    uint64_t valueSize;
};
}// namespace NES::Runtime::Execution::Operators
#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINPROBE_HPP_
