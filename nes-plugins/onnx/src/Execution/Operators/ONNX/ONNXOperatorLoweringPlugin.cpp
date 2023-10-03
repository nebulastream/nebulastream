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
#include <Execution/Operators/ONNX/ONNXInferenceOperator.hpp>
#include <Execution/Operators/ONNX/ONNXInferenceOperatorHandler.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Phases/Translations/NautilusOperatorLoweringPlugin.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class ONNXOperatorLoweringPlugin : public QueryCompilation::NautilusOperatorLoweringPlugin {
  public:
    ONNXOperatorLoweringPlugin() { NES_INFO("Load ONNXOperatorLoweringPlugin"); }

    std::optional<Runtime::Execution::Operators::ExecutableOperatorPtr>
    lower(const QueryCompilation::PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
          std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) override {
        if (!physicalOperator->instanceOf<QueryCompilation::PhysicalOperators::PhysicalInferModelOperator>()) {
            return {};
        }
        NES_INFO("Lower infer model operator to ONNX operator");
        auto inferModelOperator = physicalOperator->as<QueryCompilation::PhysicalOperators::PhysicalInferModelOperator>();
        auto model = inferModelOperator->getModel();
        // Only accept ONNX Models (.onnx file suffix)
        if (!model.ends_with(".onnx")) {
            NES_DEBUG("Model {} is not a ONNX model", model);
            return {};
        }

        //Fetch the name of input fields
        std::vector<std::string> inputFields;
        for (const auto& inputField : inferModelOperator->getInputFields()) {
            auto fieldAccessExpression = inputField->as<FieldAccessExpressionNode>();
            inputFields.push_back(fieldAccessExpression->getFieldName());
        }

        //Fetch the name of output fields
        std::vector<std::string> outputFields;
        for (const auto& outputField : inferModelOperator->getOutputFields()) {
            auto fieldAccessExpression = outputField->as<FieldAccessExpressionNode>();
            outputFields.push_back(fieldAccessExpression->getFieldName());
        }

        //build the handler to invoke model during execution
        auto handler = std::make_shared<Runtime::Execution::Operators::ONNXInferenceOperatorHandler>(model);
        operatorHandlers.push_back(handler);
        auto indexForThisHandler = operatorHandlers.size() - 1;

        //build nautilus infer model operator
        return std::make_shared<Runtime::Execution::Operators::ONNXInferenceOperator>(indexForThisHandler,
                                                                                      inputFields,
                                                                                      outputFields);
    }
};

// Register ONNX plugin
[[maybe_unused]] static QueryCompilation::NautilusOperatorLoweringPluginRegistry::Add<ONNXOperatorLoweringPlugin>
    ONNXOperatorLoweringPlugin;

}// namespace NES::Runtime::Execution::Operators