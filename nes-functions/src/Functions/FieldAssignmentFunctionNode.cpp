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
#include <memory>
#include <sstream>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/FieldAssignmentFunctionNode.hpp>
#include <Functions/FieldRenameFunctionNode.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>


namespace NES
{
FieldAssignmentFunctionNode::FieldAssignmentFunctionNode(DataTypePtr stamp) : BinaryFunctionNode(std::move(stamp)) {};

FieldAssignmentFunctionNode::FieldAssignmentFunctionNode(FieldAssignmentFunctionNode* other) : BinaryFunctionNode(other) {};

FieldAssignmentFunctionNodePtr
FieldAssignmentFunctionNode::create(const FieldAccessFunctionNodePtr& fieldAccess, const FunctionNodePtr& functionNodePtr)
{
    auto fieldAssignment = std::make_shared<FieldAssignmentFunctionNode>(functionNodePtr->getStamp());
    fieldAssignment->setChildren(fieldAccess, functionNodePtr);
    return fieldAssignment;
}

bool FieldAssignmentFunctionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<FieldAssignmentFunctionNode>(rhs))
    {
        auto otherFieldAssignment = NES::Util::as<FieldAssignmentFunctionNode>(rhs);
        /// a field assignment function has always two children.
        return getField()->equal(otherFieldAssignment->getField()) && getAssignment()->equal(otherFieldAssignment->getAssignment());
    }
    return false;
}

std::string FieldAssignmentFunctionNode::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "=" << children[1]->toString();
    return ss.str();
}

FieldAccessFunctionNodePtr FieldAssignmentFunctionNode::getField() const
{
    return Util::as<FieldAccessFunctionNode>(getLeft());
}

FunctionNodePtr FieldAssignmentFunctionNode::getAssignment() const
{
    return getRight();
}

void FieldAssignmentFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of assignment function
    getAssignment()->inferStamp(schema);

    /// field access
    auto field = getField();

    ///Update the field name with fully qualified field name
    auto fieldName = field->getFieldName();
    auto existingField = schema->getField(fieldName);
    if (existingField)
    {
        field->updateFieldName(existingField->getName());
        field->setStamp(existingField->getDataType());
    }
    else
    {
        ///Since this is a new field add the source name from schema
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

    if (field->getStamp()->isUndefined())
    {
        /// if the field has no stamp set it to the one of the assignment
        field->setStamp(getAssignment()->getStamp());
    }
    else
    {
        /// the field already has a type, check if it is compatible with the assignment
        field->getStamp()->equals(getAssignment()->getStamp());
    }
}
FunctionNodePtr FieldAssignmentFunctionNode::copy()
{
    return FieldAssignmentFunctionNode::create(Util::as<FieldAccessFunctionNode>(getField()->copy()), getAssignment()->copy());
}

} /// namespace NES
