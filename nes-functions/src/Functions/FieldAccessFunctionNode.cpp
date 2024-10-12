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
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{
FieldAccessFunctionNode::FieldAccessFunctionNode(DataTypePtr stamp, std::string fieldName)
    : FunctionNode(std::move(stamp)), fieldName(std::move(fieldName)) {};

FieldAccessFunctionNode::FieldAccessFunctionNode(FieldAccessFunctionNode* other)
    : FunctionNode(other), fieldName(other->getFieldName()) {};

FunctionNodePtr FieldAccessFunctionNode::create(DataTypePtr stamp, std::string fieldName)
{
    return std::make_shared<FieldAccessFunctionNode>(FieldAccessFunctionNode(std::move(stamp), std::move(fieldName)));
}

FunctionNodePtr FieldAccessFunctionNode::create(std::string fieldName)
{
    return create(DataTypeFactory::createUndefined(), std::move(fieldName));
}

bool FieldAccessFunctionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<FieldAccessFunctionNode>(rhs))
    {
        auto otherFieldRead = NES::Util::as<FieldAccessFunctionNode>(rhs);
        return otherFieldRead->fieldName == fieldName && otherFieldRead->stamp->equals(stamp);
    }
    return false;
}

std::string FieldAccessFunctionNode::getFieldName() const
{
    return fieldName;
}

void FieldAccessFunctionNode::updateFieldName(std::string fieldName)
{
    this->fieldName = std::move(fieldName);
}

std::string FieldAccessFunctionNode::toString() const
{
    return "FieldAccessNode(" + fieldName + "[" + stamp->toString() + "])";
}

void FieldAccessFunctionNode::inferStamp(SchemaPtr schema)
{
    /// check if the access field is defined in the schema.
    auto existingField = schema->getField(fieldName);
    if (existingField)
    {
        fieldName = existingField->getName();
        stamp = existingField->getDataType();
        return;
    }
    throw std::logic_error("FieldAccessFunction: the field " + fieldName + " is not defined in the schema " + schema->toString());
}

FunctionNodePtr FieldAccessFunctionNode::copy()
{
    return std::make_shared<FieldAccessFunctionNode>(*this);
}
} /// namespace NES
