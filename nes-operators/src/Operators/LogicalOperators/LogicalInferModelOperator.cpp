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

#include <API/AttributeField.hpp>

#include <filesystem>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/FieldAssignmentFunctionNode.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::InferModel
{

LogicalInferModelOperator::LogicalInferModelOperator(
    std::string model, std::vector<FunctionNodePtr> inputFields, std::vector<FunctionNodePtr> outputFields, OperatorId id)
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

OperatorPtr LogicalInferModelOperator::copy()
{
    auto copy = LogicalOperatorFactory::createInferModelOperator(model, inputFields, outputFields, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}
bool LogicalInferModelOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<LogicalInferModelOperator>(rhs))
    {
        auto inferModelOperator = NES::Util::as<LogicalInferModelOperator>(rhs);
        return this->getDeployedModelPath() == inferModelOperator->getDeployedModelPath();
    }
    return false;
}

bool LogicalInferModelOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalInferModelOperator>(rhs)->getId() == id;
}

void LogicalInferModelOperator::updateToFullyQualifiedFieldName(FieldAccessFunctionNodePtr field) const
{
    auto schema = getInputSchema();
    auto fieldName = field->getFieldName();
    auto existingField = schema->getField(fieldName);
    if (existingField)
    {
        field->updateFieldName(existingField->getName());
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

    auto inputSchema = getInputSchema();

    for (auto inputField : inputFields)
    {
        auto inputFunction = NES::Util::as<FieldAccessFunctionNode>(inputField);
        updateToFullyQualifiedFieldName(inputFunction);
        inputFunction->inferStamp(inputSchema);
        auto fieldName = inputFunction->getFieldName();
        inputSchema->replaceField(fieldName, inputFunction->getStamp());
    }

    for (auto outputField : outputFields)
    {
        auto outputFunction = NES::Util::as<FieldAccessFunctionNode>(outputField);
        updateToFullyQualifiedFieldName(outputFunction);
        auto fieldName = outputFunction->getFieldName();
        if (outputSchema->getField(fieldName))
        {
            /// The assigned field is part of the current schema.
            /// Thus we check if it has the correct type.
            NES_TRACE("Infer Model Logical Operator: the field {} is already in the schema, so we updated its type.", fieldName);
            outputSchema->replaceField(fieldName, outputFunction->getStamp());
        }
        else
        {
            /// The assigned field is not part of the current schema.
            /// Thus we extend the schema by the new attribute.
            NES_TRACE("Infer Model Logical Operator: the field {} is not part of the schema, so we added it.", fieldName);
            outputSchema->addField(fieldName, outputFunction->getStamp());
        }
    }

    return true;
}

void LogicalInferModelOperator::inferStringSignature()
{
    OperatorPtr operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("InferModelOperator: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty(), "LogicalInferModelOperator: InferModel should have children (?)");
    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "INFER_MODEL(" + model + ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

const std::string& LogicalInferModelOperator::getModel() const
{
    return model;
}

const std::string LogicalInferModelOperator::getDeployedModelPath() const
{
    auto idx = model.find_last_of('/');
    auto path = model;

    /// If there exist a / in the model path name. If so, then we have to remove the path to only get the file name
    if (idx != std::string::npos)
    {
        path = model.substr(idx + 1);
    }

    path = std::filesystem::temp_directory_path().string() + "/" + path;
    return path;
}

const std::vector<FunctionNodePtr>& LogicalInferModelOperator::getInputFields() const
{
    return inputFields;
}

const std::vector<FunctionNodePtr>& LogicalInferModelOperator::getOutputFields() const
{
    return outputFields;
}

} /// namespace NES::InferModel
