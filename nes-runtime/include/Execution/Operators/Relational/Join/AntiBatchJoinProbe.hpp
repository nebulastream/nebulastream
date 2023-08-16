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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_ANTIBATCHJOINPROBE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_ANTIBATCHJOINPROBE_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Relational/Join/AbstractBatchJoinProbe.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Anti batch join probe operator.
 * The operator receives input records and uses their key to probe a global hash table.
 */
class AntiBatchJoinProbe : public AbstractBatchJoinProbe {
  public:
    /**
     * @brief Creates a anti batch join probe operator.
     * @param operatorHandlerIndex index of the operator handler.
     * @param keyExpressions expressions that extract the key fields from the input record.
     * @param keyDataTypes data types of the key fields.
     * @param probeFieldIdentifiers record identifier of the value field in the probe table
     * @param valueDataTypes data types of the value fields
     * @param hashFunction hash function
     * @param resultFieldIdentifiers record identifier of the result fields
     */
    AntiBatchJoinProbe(uint64_t operatorHandlerIndex,
                   const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                   const std::vector<PhysicalTypePtr>& keyDataTypes,
                   const std::vector<Record::RecordFieldIdentifier>& probeFieldIdentifiers,
                   const std::vector<PhysicalTypePtr>& valueDataTypes,
                       std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction,
                       const std::vector<Record::RecordFieldIdentifier>& resultRecordFieldIdentifiers);

    void execute(ExecutionContext& ctx, Record& record) const override;
    void terminate(NES::Runtime::Execution::ExecutionContext& executionCtx) const override;

  private:
    static void mark(const Interface::ChainedHashMapRef::EntryRef& entry);
    static bool isMarked(const Interface::ChainedHashMapRef::EntryRef& entry);
    void writeEntryIntoRecord(const Interface::ChainedHashMapRef::EntryRef& entry, NES::Nautilus::Record& record) const;

    const std::vector<Record::RecordFieldIdentifier>& resultRecordFieldIdentifiers;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_ANTIBATCHJOINPROBE_HPP_
