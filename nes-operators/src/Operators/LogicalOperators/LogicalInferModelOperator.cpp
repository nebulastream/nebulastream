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
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::InferModel
{

LogicalInferModelOperator::LogicalInferModelOperator(
    std::string model,
    std::vector<std::shared_ptr<NodeFunction>> inputFields,
    std::vector<std::shared_ptr<NodeFunction>> outputFields,
    OperatorId id)
    : Operator(id)
    , LogicalUnaryOperator(id)
    , model(std::move(model))
    , inputFields(std::move(inputFields))
    , outputFields(std::move(outputFields))
{
    NES_DEBUG("LogicalInferModelOperator: reading from model {}", this->model);
}

std::string LogicalInferModelOperator::toString() const
{
    std::stringstream ss;
    ss << "INFER_MODEL(" << id << ")";
    return ss.str();
}

std::shared_ptr<Operator> LogicalInferModelOperator::copy()
{
    auto copy = std::make_shared<LogicalInferModelOperator>(model, inputFields, outputFields, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
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
        return this->getDeployedModelPath() == inferModelOperator->getDeployedModelPath();
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
    const auto existingField = schema.getFieldByName(fieldName);
    if (existingField)
    {
        field->updateFieldName(existingField.value().name);
    }
    else
    {
        ///Since this is a new field add the stream name from schema
        ///Check if field name is already fully qualified
        if (std::ranges::size(fieldName) > 1)
        {
            field->updateFieldName(fieldName);
        }
        else
        {
            field->updateFieldName(schema.getCommonPrefix() + fieldName);
        }
    }
}

bool LogicalInferModelOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }

    auto inputSchema = getInputSchema();

    for (auto inputField : inputFields)
    {
        auto inputFunction = NES::Util::as<NodeFunctionFieldAccess>(inputField);
        updateToFullyQualifiedFieldName(inputFunction);
        inputFunction->inferStamp(inputSchema);
        auto fieldName = inputFunction->getFieldName();
        inputSchema.replaceTypeOfField(fieldName, inputFunction->getStamp());
    }

    for (auto outputField : outputFields)
    {
        auto outputFunction = NES::Util::as<NodeFunctionFieldAccess>(outputField);
        updateToFullyQualifiedFieldName(outputFunction);
        auto fieldName = outputFunction->getFieldName();
        if (outputSchema.getFieldByName(fieldName))
        {
            /// The assigned field is part of the current schema.
            /// Thus we check if it has the correct type.
            NES_TRACE("Infer Model Logical Operator: the field {} is already in the schema, so we updated its type.", fieldName);
            outputSchema.replaceTypeOfField(fieldName, outputFunction->getStamp());
        }
        else
        {
            /// The assigned field is not part of the current schema.
            /// Thus we extend the schema by the new attribute.
            NES_TRACE("Infer Model Logical Operator: the field {} is not part of the schema, so we added it.", fieldName);
            outputSchema.addField(fieldName, outputFunction->getStamp());
        }
    }

    return true;
}


const std::string& LogicalInferModelOperator::getModel() const
{
    return model;
}

const std::string LogicalInferModelOperator::getDeployedModelPath() const
{
    const auto idx = model.find_last_of('/');
    auto path = model;

    /// If there exist a / in the model path name. If so, then we have to remove the path to only get the file name
    if (idx != std::string::npos)
    {
        path = model.substr(idx + 1);
    }

    path = std::filesystem::temp_directory_path().string() + "/" + path;
    return path;
}

const std::vector<std::shared_ptr<NodeFunction>>& LogicalInferModelOperator::getInputFields() const
{
    return inputFields;
}

const std::vector<std::shared_ptr<NodeFunction>>& LogicalInferModelOperator::getOutputFields() const
{
    return outputFields;
}

}
