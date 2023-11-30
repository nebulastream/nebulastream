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
 * @brief Project operator that maps the field names of a record from input field names to output field names.
 */
class Project : public ExecutableOperator {
  public:
    /**
     * @brief Constructs a project operator (maps field names of a record(input) to new field names(output))
     * @param inputFields: The input field names that must be reflected in the record during execute()
     * @param inputFields: The output field names that the input field names of the record are mapped to during execute()
     */
    Project(const std::vector<Record::RecordFieldIdentifier>& inputFields,
            const std::vector<Record::RecordFieldIdentifier>& outputFields)
        : inputFields(inputFields), outputFields(outputFields){};
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const std::vector<Record::RecordFieldIdentifier> inputFields;
    const std::vector<Record::RecordFieldIdentifier> outputFields;
};

}// namespace NES::Runtime::Execution::Operators
#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PROJECT_HPP_
