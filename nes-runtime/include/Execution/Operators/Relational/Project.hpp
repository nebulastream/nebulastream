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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PROJECT_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PROJECT_HPP_
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Project operator that evaluates a project expression on a input records. todo
 * Project expressions read record fields, apply transformations, and can set/update fields.
 */
class Project : public ExecutableOperator {
  public:
    /**
     * @brief Creates a project operator
     * @param todo
     */
    Project(const std::vector<Record::RecordFieldIdentifier>& inputFields, const std::vector<Record::RecordFieldIdentifier>& outputFields) 
        : inputFields(inputFields), outputFields(outputFields){};
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const std::vector<Record::RecordFieldIdentifier> inputFields;
    const std::vector<Record::RecordFieldIdentifier> outputFields;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PROJECT_HPP_
