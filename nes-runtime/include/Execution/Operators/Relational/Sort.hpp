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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORT_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORT_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Expressions/Expression.hpp>

namespace NES::Runtime::Execution::Operators {

class Sort : public ExecutableOperator {
  public:

    Sort(const uint64_t operatorHandlerIndex,
         const std::vector<Expressions::ExpressionPtr>& sortExpressions,
         const std::vector<PhysicalTypePtr>& sortDataTypes);

    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void close(ExecutionContext& executionCtx,  RecordBuffer& recordBuffer) const override;

  private:
    const std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider;
    uint64_t keySize;

    const uint64_t operatorHandlerIndex;
    const std::vector<Expressions::ExpressionPtr> sortExpressions;
    const std::vector<PhysicalTypePtr> sortDataTypes;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORT_HPP_
