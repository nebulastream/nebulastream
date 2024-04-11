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
#include <sstream>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalInferModelOperator::PhysicalInferModelOperator(OperatorId id,
                                                       StatisticId statisticId,
                                                       SchemaPtr inputSchema,
                                                       SchemaPtr outputSchema,
                                                       std::string model,
                                                       std::vector<ExpressionNodePtr> inputFields,
                                                       std::vector<ExpressionNodePtr> outputFields)
    : Operator(id, statisticId), PhysicalUnaryOperator(id, statisticId, inputSchema, outputSchema), model(model), inputFields(inputFields),
      outputFields(outputFields) {}

PhysicalOperatorPtr PhysicalInferModelOperator::create(OperatorId id,
                                                       StatisticId statisticId,
                                                       SchemaPtr inputSchema,
                                                       SchemaPtr outputSchema,
                                                       std::string model,
                                                       std::vector<ExpressionNodePtr> inputFields,
                                                       std::vector<ExpressionNodePtr> outputFields) {
    return std::make_shared<PhysicalInferModelOperator>(id, statisticId, inputSchema, outputSchema, model, inputFields, outputFields);
}

PhysicalOperatorPtr PhysicalInferModelOperator::create(StatisticId statisticId,
                                                       SchemaPtr inputSchema,
                                                       SchemaPtr outputSchema,
                                                       std::string model,
                                                       std::vector<ExpressionNodePtr> inputFields,
                                                       std::vector<ExpressionNodePtr> outputFields) {
    return create(getNextOperatorId(), statisticId, inputSchema, outputSchema, model, inputFields, outputFields);
}

std::string PhysicalInferModelOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalInferModelOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << "model: " << model;
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalInferModelOperator::copy() {
    auto result = create(id, statisticId, inputSchema, outputSchema, model, inputFields, outputFields);
    result->addAllProperties(properties);
    return result;
}

const std::string& PhysicalInferModelOperator::getModel() const { return model; }
const std::vector<ExpressionNodePtr>& PhysicalInferModelOperator::getInputFields() const { return inputFields; }
const std::vector<ExpressionNodePtr>& PhysicalInferModelOperator::getOutputFields() const { return outputFields; }

}// namespace NES::QueryCompilation::PhysicalOperators
