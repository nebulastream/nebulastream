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

#ifndef NES_RUNTIME_EXECUTION_OPERATOR_INFERMODEL_HPP
#define NES_RUNTIME_EXECUTION_OPERATOR_INFERMODEL_HPP

#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES {

class ExpressionItem;
using ExpressionItemPtr = std::shared_ptr<ExpressionItem>;

namespace Runtime::Execution::Operators {

class InferModel : public ExecutableOperator {

  public:
    /**
     * @brief Creates a infer model operator.
     * @param inferModelHandlerIndex index of infer model handler.
     * @param inputFields input fields for the model inference
     * @param outputFields output fields from the model inference
     */
    InferModel(const uint32_t inferModelHandlerIndex,
               const std::vector<ExpressionItemPtr>& inputFields,
               const std::vector<ExpressionItemPtr>& outputFields)
        : inferModelHandlerIndex(inferModelHandlerIndex), inputFields(inputFields), outputFields(outputFields){};

    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const uint32_t inferModelHandlerIndex;
    const std::vector<ExpressionItemPtr> inputFields;
    const std::vector<ExpressionItemPtr> outputFields;
};

}// namespace Runtime::Execution::Operators
}// namespace NES
#endif//NES_RUNTIME_EXECUTION_OPERATOR_INFERMODEL_HPP
