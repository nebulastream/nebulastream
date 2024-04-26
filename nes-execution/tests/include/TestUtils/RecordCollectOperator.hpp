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

#ifndef NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_RECORDCOLLECTOPERATOR_HPP_
#define NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_RECORDCOLLECTOPERATOR_HPP_
#include <Execution/Operators/ExecutableOperator.hpp>
namespace NES::Runtime::Execution::Operators {

class CollectOperator;
using CollectOperatorPtr = std::shared_ptr<CollectOperator>;

class CollectOperator : public ExecutableOperator {
  public:
    CollectOperator() = default;
    void execute(ExecutionContext&, Record& record) const override;
    mutable std::vector<Record> records;
};

}// namespace NES::Runtime::Execution::Operators

#endif // NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_RECORDCOLLECTOPERATOR_HPP_
