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
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentBinaryLogicalFunction.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Undefined.hpp>

namespace NES
{
FieldAssignmentBinaryLogicalFunction::FieldAssignmentBinaryLogicalFunction(std::shared_ptr<DataType> stamp)
    : BinaryLogicalFunction(std::move(stamp), "FieldAssignment") {};

FieldAssignmentBinaryLogicalFunction::FieldAssignmentBinaryLogicalFunction(FieldAssignmentBinaryLogicalFunction* other)
    : BinaryLogicalFunction(other) {};

std::shared_ptr<FieldAssignmentBinaryLogicalFunction> FieldAssignmentBinaryLogicalFunction::create(
    const std::shared_ptr<FieldAccessLogicalFunction>& fieldAccess, const std::shared_ptr<LogicalFunction>& LogicalFunction)
{
    auto fieldAssignment = std::make_shared<FieldAssignmentBinaryLogicalFunction>(LogicalFunction->getStamp());
    fieldAssignment->setChildren(fieldAccess, LogicalFunction);
    return fieldAssignment;
}

bool FieldAssignmentBinaryLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<FieldAssignmentBinaryLogicalFunction>(rhs))
    {
        const auto otherFieldAssignment = NES::Util::as<FieldAssignmentBinaryLogicalFunction>(rhs);
        /// a field assignment function has always two children.
        const bool fieldsMatch = getField()->equal(otherFieldAssignment->getField());
        const bool assignmentsMatch = getAssignment()->equal(otherFieldAssignment->getAssignment());
        return fieldsMatch and assignmentsMatch;
    }
    return false;
}

std::string FieldAssignmentBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "FieldAssignmentBinaryLogicalFunction(" << *children[0] << "=" << *children[1] << ")";
    return ss.str();
}

NodeFunctionFieldAccessPtr NodeFunctionFieldAssignment::getField() const
{
    return Util::as<FieldAccessLogicalFunction>(getLeft());
}

std::shared_ptr<LogicalFunction> FieldAssignmentBinaryLogicalFunction::getAssignment() const
{
    return getRight();
}

void FieldAssignmentBinaryLogicalFunction::inferStamp(const Schema& schema)
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

std::shared_ptr<LogicalFunction> FieldAssignmentBinaryLogicalFunction::deepCopy()
{
    return FieldAssignmentBinaryLogicalFunction::create(
        Util::as<std::shared_ptr<FieldAccessLogicalFunction>>(getField()->deepCopy()), getAssignment()->deepCopy());
}

bool FieldAssignmentBinaryLogicalFunction::validateBeforeLowering() const
{
    return children.empty();
}
}
