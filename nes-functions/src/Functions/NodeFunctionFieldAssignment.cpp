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
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldAssignment.cpp
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
========
#include <Functions/FieldAssignmentFunctionNode.hpp>
#include <Functions/FieldRenameFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldAssignmentFunctionNode.cpp
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldAssignment.cpp
NodeFunctionFieldAssignment::NodeFunctionFieldAssignment(DataTypePtr stamp) : NodeFunctionBinary(std::move(stamp), "FieldAssignment") {};

NodeFunctionFieldAssignment::NodeFunctionFieldAssignment(NodeFunctionFieldAssignment* other) : NodeFunctionBinary(other) {};

NodeFunctionFieldAssignmentPtr
NodeFunctionFieldAssignment::create(const NodeFunctionFieldAccessPtr& fieldAccess, const NodeFunctionPtr& NodeFunctionPtr)
{
    auto fieldAssignment = std::make_shared<NodeFunctionFieldAssignment>(NodeFunctionPtr->getStamp());
    fieldAssignment->setChildren(fieldAccess, NodeFunctionPtr);
    return fieldAssignment;
}

bool NodeFunctionFieldAssignment::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionFieldAssignment>(rhs))
    {
        auto otherFieldAssignment = NES::Util::as<NodeFunctionFieldAssignment>(rhs);
========
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
    if (rhs->instanceOf<FieldAssignmentFunctionNode>())
    {
        auto otherFieldAssignment = rhs->as<FieldAssignmentFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldAssignmentFunctionNode.cpp
        /// a field assignment function has always two children.
        return getField()->equal(otherFieldAssignment->getField()) && getAssignment()->equal(otherFieldAssignment->getAssignment());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldAssignment.cpp
std::string NodeFunctionFieldAssignment::toString() const
========
std::string FieldAssignmentFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldAssignmentFunctionNode.cpp
{
    std::stringstream ss;
    ss << children[0]->toString() << "=" << children[1]->toString();
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldAssignment.cpp
NodeFunctionFieldAccessPtr NodeFunctionFieldAssignment::getField() const
{
    return Util::as<NodeFunctionFieldAccess>(getLeft());
}

NodeFunctionPtr NodeFunctionFieldAssignment::getAssignment() const
========
FieldAccessFunctionNodePtr FieldAssignmentFunctionNode::getField() const
{
    return getLeft()->as<FieldAccessFunctionNode>();
}

FunctionNodePtr FieldAssignmentFunctionNode::getAssignment() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldAssignmentFunctionNode.cpp
{
    return getRight();
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldAssignment.cpp
void NodeFunctionFieldAssignment::inferStamp(SchemaPtr schema)
========
void FieldAssignmentFunctionNode::inferStamp(SchemaPtr schema)
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldAssignmentFunctionNode.cpp
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
        const auto stamp = getAssignment()->getStamp()->join(field->getStamp());
        field->updateFieldName(existingField->getName());
        field->setStamp(stamp);
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
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldAssignment.cpp
NodeFunctionPtr NodeFunctionFieldAssignment::deepCopy()
{
    return NodeFunctionFieldAssignment::create(Util::as<NodeFunctionFieldAccess>(getField()->deepCopy()), getAssignment()->deepCopy());
========
FunctionNodePtr FieldAssignmentFunctionNode::copy()
{
    return FieldAssignmentFunctionNode::create(getField()->copy()->as<FieldAccessFunctionNode>(), getAssignment()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldAssignmentFunctionNode.cpp
}

bool NodeFunctionFieldAssignment::validateBeforeLowering() const
{
    return children.empty();
}
}
