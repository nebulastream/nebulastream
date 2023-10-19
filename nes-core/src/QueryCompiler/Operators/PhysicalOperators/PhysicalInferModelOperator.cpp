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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalInferModelOperator::PhysicalInferModelOperator(OperatorId id,
                                                       SchemaPtr inputSchema,
                                                       SchemaPtr outputSchema,
                                                       std::string model,
                                                       std::vector<ExpressionNodePtr> inputFields,
                                                       std::vector<ExpressionNodePtr> outputFields,
                                                       InferModel::InferModelOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), model(model), inputFields(inputFields),
      outputFields(outputFields), operatorHandler(operatorHandler) {}

PhysicalOperatorPtr PhysicalInferModelOperator::create(OperatorId id,
                                                       SchemaPtr inputSchema,
                                                       SchemaPtr outputSchema,
                                                       std::string model,
                                                       std::vector<ExpressionNodePtr> inputFields,
                                                       std::vector<ExpressionNodePtr> outputFields,
                                                       InferModel::InferModelOperatorHandlerPtr operatorHandler) {
    return std::make_shared<PhysicalInferModelOperator>(id,
                                                        inputSchema,
                                                        outputSchema,
                                                        model,
                                                        inputFields,
                                                        outputFields,
                                                        operatorHandler);
}

PhysicalOperatorPtr PhysicalInferModelOperator::create(SchemaPtr inputSchema,
                                                       SchemaPtr outputSchema,
                                                       std::string model,
                                                       std::vector<ExpressionNodePtr> inputFields,
                                                       std::vector<ExpressionNodePtr> outputFields,
                                                       InferModel::InferModelOperatorHandlerPtr operatorHandler) {
    return create(getNextOperatorId(), inputSchema, outputSchema, model, inputFields, outputFields, operatorHandler);
}

std::string PhysicalInferModelOperator::toString() const { return "PhysicalInferModelOperator"; }

OperatorNodePtr PhysicalInferModelOperator::copy() {
    auto result = create(id, inputSchema, outputSchema, model, inputFields, outputFields, operatorHandler);
    result->addAllProperties(properties);
    return result;
}

const std::string& PhysicalInferModelOperator::getModel() const { return model; }
const std::vector<ExpressionNodePtr>& PhysicalInferModelOperator::getInputFields() const { return inputFields; }
const std::vector<ExpressionNodePtr>& PhysicalInferModelOperator::getOutputFields() const { return outputFields; }

InferModel::InferModelOperatorHandlerPtr PhysicalInferModelOperator::getInferModelHandler() { return operatorHandler; }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES