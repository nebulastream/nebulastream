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
#include <API/Expressions/Expressions.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Tensorflow/TensorflowInferenceOperator.hpp>
#include <Execution/Operators/Tensorflow/TensorflowInferenceOperatorHandler.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Phases/Translations/NautilusOperatorLoweringPlugin.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <bits/shared_ptr.h>

namespace NES::Runtime::Execution::Operators {

class TensorflowOperatorLoweringPlugin : public QueryCompilation::NautilusOperatorLoweringPlugin {
  public:
    TensorflowOperatorLoweringPlugin(){
        NES_INFO("Load TensorflowOperatorLoweringPlugin");
    }
    std::optional<Runtime::Execution::Operators::ExecutableOperatorPtr>
    lower(const QueryCompilation::PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
          std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) override {
        if (!physicalOperator->instanceOf<QueryCompilation::PhysicalOperators::PhysicalInferModelOperator>()) {
            return {};
        }
        NES_INFO("Lower infer model operator to Tensorflow operator");
        auto inferModelOperator = physicalOperator->as<QueryCompilation::PhysicalOperators::PhysicalInferModelOperator>();
        auto model = inferModelOperator->getModel();

        //Fetch the name of input fields
        std::vector<std::string> inputFields;
        for (const auto& inputField : inferModelOperator->getInputFields()) {
            auto fieldAccessExpression = inputField->getExpressionNode()->as<FieldAccessExpressionNode>();
            inputFields.push_back(fieldAccessExpression->getFieldName());
        }

        //Fetch the name of output fields
        std::vector<std::string> outputFields;
        for (const auto& outputField : inferModelOperator->getOutputFields()) {
            auto fieldAccessExpression = outputField->getExpressionNode()->as<FieldAccessExpressionNode>();
            outputFields.push_back(fieldAccessExpression->getFieldName());
        }

        //build the handler to invoke model during execution
        auto handler = std::make_shared<Runtime::Execution::Operators::TensorflowInferenceOperatorHandler>(model);
        operatorHandlers.push_back(handler);
        auto indexForThisHandler = operatorHandlers.size() - 1;

        //build nautilus infer model operator
        return std::make_shared<Runtime::Execution::Operators::TensorflowInferenceOperator>(indexForThisHandler,
                                                                                            inputFields,
                                                                                            outputFields);
    }
};

// Register tensorflow plugin
[[maybe_unused]] static QueryCompilation::NautilusOperatorLoweringPluginRegistry::Add<TensorflowOperatorLoweringPlugin>
    tensorflowOperatorLoweringPlugin;

}// namespace NES::Runtime::Execution::Operators