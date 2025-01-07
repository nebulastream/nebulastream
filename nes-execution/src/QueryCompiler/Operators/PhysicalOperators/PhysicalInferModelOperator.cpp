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
#include <sstream>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalInferModelOperator::PhysicalInferModelOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    std::string model,
    std::vector<NodeFunctionPtr> inputFields,
    std::vector<NodeFunctionPtr> outputFields)
    : Operator(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), model(model), inputFields(inputFields), outputFields(outputFields)
{
}

PhysicalOperatorPtr PhysicalInferModelOperator::create(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    std::string model,
    std::vector<NodeFunctionPtr> inputFields,
    std::vector<NodeFunctionPtr> outputFields)
{
    return std::make_shared<PhysicalInferModelOperator>(id, inputSchema, outputSchema, model, inputFields, outputFields);
}

PhysicalOperatorPtr PhysicalInferModelOperator::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    std::string model,
    std::vector<NodeFunctionPtr> inputFields,
    std::vector<NodeFunctionPtr> outputFields)
{
    return create(getNextOperatorId(), inputSchema, outputSchema, model, inputFields, outputFields);
}

std::string PhysicalInferModelOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalInferModelOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << "model: " << model;
    out << std::endl;
    return out.str();
}

std::shared_ptr<Operator> PhysicalInferModelOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema, model, inputFields, outputFields);
    result->addAllProperties(properties);
    return result;
}

const std::string& PhysicalInferModelOperator::getModel() const
{
    return model;
}
const std::vector<NodeFunctionPtr>& PhysicalInferModelOperator::getInputFields() const
{
    return inputFields;
}
const std::vector<NodeFunctionPtr>& PhysicalInferModelOperator::getOutputFields() const
{
    return outputFields;
}

}
