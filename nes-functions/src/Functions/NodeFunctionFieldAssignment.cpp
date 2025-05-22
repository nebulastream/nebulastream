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
#include <ostream>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Undefined.hpp>

namespace NES
{
NodeFunctionFieldAssignment::NodeFunctionFieldAssignment(std::shared_ptr<DataType> stamp)
    : NodeFunctionBinary(std::move(stamp), "FieldAssignment") { };

NodeFunctionFieldAssignment::NodeFunctionFieldAssignment(NodeFunctionFieldAssignment* other) : NodeFunctionBinary(other) { };

std::shared_ptr<NodeFunctionFieldAssignment> NodeFunctionFieldAssignment::create(
    const std::shared_ptr<NodeFunctionFieldAccess>& fieldAccess, const std::shared_ptr<NodeFunction>& nodeFunction)
{
    auto fieldAssignment = std::make_shared<NodeFunctionFieldAssignment>(nodeFunction->getStamp());
    fieldAssignment->setChildren(fieldAccess, nodeFunction);
    return fieldAssignment;
}

bool NodeFunctionFieldAssignment::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionFieldAssignment>(rhs))
    {
        const auto otherFieldAssignment = NES::Util::as<NodeFunctionFieldAssignment>(rhs);
        /// a field assignment function has always two children.
        const bool fieldsMatch = getField()->equal(otherFieldAssignment->getField());
        const bool assignmentsMatch = getAssignment()->equal(otherFieldAssignment->getAssignment());
        return fieldsMatch and assignmentsMatch;
    }
    return false;
}

std::ostream& NodeFunctionFieldAssignment::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << "NodeFunctionFieldAssignment(" << *children.at(0) << " = " << *children.at(1) << ")";
}

std::ostream& NodeFunctionFieldAssignment::toQueryPlanString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << VerbosityLevel::QueryPlan << *children.at(0) << " = " << *children.at(1);
}

std::shared_ptr<NodeFunctionFieldAccess> NodeFunctionFieldAssignment::getField() const
{
    return Util::as<NodeFunctionFieldAccess>(getLeft());
}

std::shared_ptr<NodeFunction> NodeFunctionFieldAssignment::getAssignment() const
{
    return getRight();
}

void NodeFunctionFieldAssignment::inferStamp(const Schema& schema)
{
    /// infer stamp of assignment function
    getAssignment()->inferStamp(schema);

    /// field access
    auto field = getField();

    ///Update the field name with fully qualified field name
    auto fieldName = field->getFieldName();
    auto existingField = schema.getFieldByName(fieldName);
    if (existingField)
    {
        const auto stamp = getAssignment()->getStamp()->join(field->getStamp());
        field->updateFieldName(existingField.value()->getName());
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
            field->updateFieldName(schema.getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldName);
        }
    }

    if (NES::Util::instanceOf<Undefined>(field->getStamp()))
    {
        /// if the field has no stamp set it to the one of the assignment
        field->setStamp(getAssignment()->getStamp());
    }
    else
    {
        /// the field already has a type, check if it is compatible with the assignment
        if (*field->getStamp() != *getAssignment()->getStamp())
        {
            NES_WARNING(
                "Field {} stamp is incompatible with assignment stamp. Overwriting field stamp with assignment stamp.",
                field->getFieldName())
            field->setStamp(getAssignment()->getStamp());
        }
    }
}

std::shared_ptr<NodeFunction> NodeFunctionFieldAssignment::deepCopy()
{
    return NodeFunctionFieldAssignment::create(Util::as<NodeFunctionFieldAccess>(getField()->deepCopy()), getAssignment()->deepCopy());
}

bool NodeFunctionFieldAssignment::validateBeforeLowering() const
{
    return children.empty();
}
}
