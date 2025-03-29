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
#include <API/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::InferModel
{

InferModelLogicalOperator::InferModelLogicalOperator(
    std::string model,
    std::vector<std::shared_ptr<LogicalFunction>> inputFields,
    std::vector<std::shared_ptr<LogicalFunction>> outputFields,
    OperatorId id)
    : Operator(id)
    , UnaryLogicalOperator(id)
    , model(std::move(model))
    , inputFields(std::move(inputFields))
    , outputFields(std::move(outputFields))
{
    NES_DEBUG("InferModelLogicalOperator: reading from model {}", this->model);
}

std::string InferModelLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "INFER_MODEL(" << id << ")";
    return ss.str();
}

std::shared_ptr<Operator> InferModelLogicalOperator::clone() const
{
    auto copy = std::make_shared<InferModelLogicalOperator>(model, inputFields, outputFields, id);
    copy->setInputSchema(getInputSchema());
    copy->setOutputSchema(getOutputSchema());
    return copy;
}
bool InferModelLogicalOperator::operator==(Operator const& rhs) const
{
    if (auto rhsOperator = dynamic_cast<const InferModelLogicalOperator*>(&rhs)) {
        return getDeployedModelPath() == rhsOperator->getDeployedModelPath();
    }
    return false;
}

bool InferModelLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const InferModelLogicalOperator*>(&rhs)->getId() == id;
}

void InferModelLogicalOperator::updateToFullyQualifiedFieldName(std::shared_ptr<FieldAccessLogicalFunction> field) const
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

bool InferModelLogicalOperator::inferSchema()
{
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }

    const auto inputSchema = getInputSchema();

    for (auto inputField : inputFields)
    {
        auto inputFunction = NES::Util::as<FieldAccessLogicalFunction>(inputField);
        updateToFullyQualifiedFieldName(inputFunction);
        inputFunction->inferStamp(*inputSchema);
        auto fieldName = inputFunction->getFieldName();
        inputSchema->replaceField(fieldName, inputFunction->getStamp());
    }

    for (auto outputField : outputFields)
    {
        auto outputFunction = NES::Util::as<FieldAccessLogicalFunction>(outputField);
        updateToFullyQualifiedFieldName(outputFunction);
        auto fieldName = outputFunction->getFieldName();
        if (getOutputSchema()->getFieldByName(fieldName))
        {
            /// The assigned field is part of the current schema.
            /// Thus we check if it has the correct type.
            NES_TRACE("Infer Model Logical Operator: the field {} is already in the schema, so we updated its type.", fieldName);
            getOutputSchema()->replaceField(fieldName, outputFunction->getStamp());
        }
        else
        {
            /// The assigned field is not part of the current schema.
            /// Thus we extend the schema by the new attribute.
            NES_TRACE("Infer Model Logical Operator: the field {} is not part of the schema, so we added it.", fieldName);
            getOutputSchema()->addField(fieldName, outputFunction->getStamp());
        }
    }

    return true;
}

const std::string& InferModelLogicalOperator::getModel() const
{
    return model;
}

const std::string InferModelLogicalOperator::getDeployedModelPath() const
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

const std::vector<std::shared_ptr<LogicalFunction>>& InferModelLogicalOperator::getInputFields() const
{
    return inputFields;
}

const std::vector<std::shared_ptr<LogicalFunction>>& InferModelLogicalOperator::getOutputFields() const
{
    return outputFields;
}

}
