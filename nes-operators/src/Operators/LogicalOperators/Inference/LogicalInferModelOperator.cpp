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

#include <filesystem>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/Inference/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>
#include <Model.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::InferModel
{

LogicalInferModelOperator::LogicalInferModelOperator(
    OperatorId id,
    Nebuli::Inference::Model model,
    std::vector<std::shared_ptr<NodeFunction>> inputFields)
    : Operator(id)
    , LogicalUnaryOperator(id)
    , model(std::move(model))
    , inputFields(std::move(inputFields))
{
}

namespace
{
std::string getFieldName(const NodeFunction& function)
{
    if (const auto* nodeFunctionFieldAccess = dynamic_cast<const NodeFunctionFieldAccess*>(&function))
    {
        return nodeFunctionFieldAccess->getFieldName();
    }
    return dynamic_cast<const NodeFunctionFieldAssignment*>(&function)->getField()->getFieldName();
}
}

std::string LogicalInferModelOperator::toString() const
{
    PRECONDITION(not model.getByteCode().empty(), "Inference operator must contain a path to the model.");
    PRECONDITION(not inputFields.empty(), "Inference operator must contain at least 1 input field.");

    if (not outputSchema->getFieldNames().empty())
    {
        return fmt::format("INFER_MODEL(opId: {}, schema={})", id, outputSchema->toString());
    }
    return fmt::format(
        "INFER_MODEL(opId: {}, inputFields: [{}])",
        id,
        fmt::join(std::views::transform(inputFields, [](const auto& field) { return getFieldName(*field); }), ", "));
}

std::shared_ptr<Operator> LogicalInferModelOperator::copy()
{
    auto copy = std::make_shared<LogicalInferModelOperator>(id, model, inputFields);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}
bool LogicalInferModelOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalInferModelOperator>(rhs))
    {
        const auto inferModelOperator = NES::Util::as<LogicalInferModelOperator>(rhs);
        return this->model == inferModelOperator->model;
    }
    return false;
}

bool LogicalInferModelOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalInferModelOperator>(rhs)->getId() == id;
}

void LogicalInferModelOperator::updateToFullyQualifiedFieldName(const std::shared_ptr<NodeFunctionFieldAccess>& field) const
{
    const auto schema = getInputSchema();
    const auto fieldName = field->getFieldName();
    const auto existingField = schema->getFieldByName(fieldName);
    if (existingField)
    {
        field->updateFieldName(existingField.value()->getName());
    }
    else
    {
        ///Since this is a new field add the stream name from schema
        ///Check if field name is already fully qualified
        if (fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos)
        {
            field->updateFieldName(fieldName);
        }
        else
        {
            field->updateFieldName(schema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldName);
        }
    }
}

bool LogicalInferModelOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }

    const auto inputSchema = getInputSchema();

    if (inputFields.size() != model.getInputs().size())
    {
        throw CannotInferSchema("Model expects {} inputs, but received {}", inputFields.size(), model.getInputs().size());
    }

    for (const auto& [field, expectedType] : std::views::zip(inputFields, model.getInputs()))
    {
        auto inputFunction = NES::Util::as<NodeFunctionFieldAccess>(field);
        updateToFullyQualifiedFieldName(inputFunction);
        inputFunction->inferStamp(*inputSchema);

        if (*inputFunction->getStamp() != *expectedType)
        {
            throw CannotInferSchema("Model Expected '{}', but received {}", expectedType->toString(), inputFunction->getStamp()->toString());
        }

        auto fieldName = inputFunction->getFieldName();
        inputSchema->replaceField(fieldName, inputFunction->getStamp());
    }

    for (const auto& [name, type] : model.getOutputs())
    {
        if (outputSchema->getFieldByName(name))
        {
            /// The assigned field is part of the current schema.
            /// Thus we check if it has the correct type.
            NES_TRACE("Infer Model Logical Operator: the field {} is already in the schema, so we updated its type.", name);
            outputSchema->replaceField(name, type);
        }
        else
        {
            /// The assigned field is not part of the current schema.
            /// Thus we extend the schema by the new attribute.
            NES_TRACE("Infer Model Logical Operator: the field {} is not part of the schema, so we added it.", name);
            // outputSchema->addField(fieldName, outputFunction->getStamp());
            // TODO: default all output fields to Float for now
            outputSchema->addField(name, type);
        }
    }

    return true;
}

const std::vector<std::shared_ptr<NodeFunction>>& LogicalInferModelOperator::getInputFields() const
{
    return inputFields;
}

}
