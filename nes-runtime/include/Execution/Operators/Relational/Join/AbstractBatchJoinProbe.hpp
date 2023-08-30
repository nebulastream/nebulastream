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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINPROBE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINPROBE_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Abstract batch join probe operator.
 * The operator receives input records and uses their key to probe a global hash table.
 */
class AbstractBatchJoinProbe : public ExecutableOperator {
  public:
    /**
     * @brief Creates an abstract batch join probe operator.
     * @param operatorHandlerIndex index of the operator handler.
     * @param keyExpressions expressions that extract the key fields from the input record.
     * @param keyDataTypes data types of the key fields.
     * @param buildFieldIdentifiers record identifier of the value field in the built table
     * @param valueDataTypes data types of the value fields
     * @param hashFunction hash function
     */
    AbstractBatchJoinProbe(uint64_t operatorHandlerIndex,
                   const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                   const std::vector<PhysicalTypePtr>& keyDataTypes,
                           const std::vector<Record::RecordFieldIdentifier>& buildFieldIdentifiers,
                           const std::vector<PhysicalTypePtr>& valueDataTypes,
                   std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction);
    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

    /**
     * @brief Local join probe state containing the hash map.
     */
    class LocalJoinProbeState : public Operators::OperatorState {
      public:
        explicit LocalJoinProbeState(Interface::ChainedHashMapRef hashMap) : hashMap(std::move(hashMap)){};
        Interface::ChainedHashMapRef hashMap;
    };

  protected:
    /**
     * @brief Finds the matching entry in the hash map.
     * @param ctx execution context
     * @param record record
     * @return KeyEntryIterator
     */
    Interface::ChainedHashMapRef::KeyEntryIterator findMatches(NES::Runtime::Execution::ExecutionContext& ctx,
                                                               NES::Nautilus::Record& record) const;

    const uint64_t operatorHandlerIndex;
    const std::vector<Expressions::ExpressionPtr> keyExpressions;
    const std::vector<PhysicalTypePtr> keyDataTypes;
    const std::vector<Record::RecordFieldIdentifier> buildFieldIdentifiers;
    const std::vector<PhysicalTypePtr> valueDataTypes;
    const std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction;
    uint64_t keySize;
    uint64_t valueSize;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINPROBE_HPP_
