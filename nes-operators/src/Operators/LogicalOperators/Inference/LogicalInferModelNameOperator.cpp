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
#include <Operators/LogicalOperators/Inference/LogicalInferModelNameOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::InferModel
{

std::shared_ptr<Operator> LogicalInferModelNameOperator::copy()
{
    auto copy = std::make_shared<LogicalInferModelNameOperator>(id, modelName, inputFields);
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
bool LogicalInferModelNameOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalInferModelNameOperator>(rhs))
    {
        const auto inferModelOperator = NES::Util::as<LogicalInferModelNameOperator>(rhs);
        return this->modelName == inferModelOperator->modelName;
    }
    return false;
}

bool LogicalInferModelNameOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalInferModelNameOperator>(rhs)->getId() == id;
}

void LogicalInferModelNameOperator::updateToFullyQualifiedFieldName(const std::shared_ptr<NodeFunctionFieldAccess>& field) const
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

bool LogicalInferModelNameOperator::inferSchema()
{
    throw std::runtime_error(
        "Type Inference should not be called on the LogicalInferModelName operator, as model information are not yet available");
}

void LogicalInferModelNameOperator::inferStringSignature()
{
    const std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("InferModelOperator: Inferring String signature for {}", *operatorNode);
    INVARIANT(!children.empty(), "InferModel must have children, but had none");
    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    const auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "INFER_MODEL_NAME(" + modelName + ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

const std::vector<std::shared_ptr<NodeFunction>>& LogicalInferModelNameOperator::getInputFields() const
{
    return inputFields;
}

}
