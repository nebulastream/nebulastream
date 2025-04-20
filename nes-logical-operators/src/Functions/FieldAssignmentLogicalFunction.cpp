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
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Undefined.hpp>

namespace NES
{

FieldAssignmentLogicalFunction::FieldAssignmentLogicalFunction(const FieldAssignmentLogicalFunction& other)
    : stamp(other.stamp), fieldAccess(other.fieldAccess), logicalFunction(other.logicalFunction) {};

FieldAssignmentLogicalFunction::FieldAssignmentLogicalFunction(
    const FieldAccessLogicalFunction& fieldAccess, LogicalFunction logicalFunction)
    : stamp(logicalFunction.getStamp().clone()), fieldAccess(fieldAccess), logicalFunction(logicalFunction)
{
}

bool FieldAssignmentLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const FieldAssignmentLogicalFunction*>(&rhs))
    {
        /// a field assignment function has always two children.
        const bool fieldsMatch = getField() == other->getField();
        const bool assignmentsMatch = getAssignment() == other->getAssignment();
        return fieldsMatch and assignmentsMatch;
    }
    return false;
}

std::string FieldAssignmentLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "FieldAssignmentLogicalFunction(" << fieldAccess << "=" << logicalFunction << ")";
    return ss.str();
}

FieldAccessLogicalFunction FieldAssignmentLogicalFunction::getField() const
{
    return fieldAccess;
}

LogicalFunction FieldAssignmentLogicalFunction::getAssignment() const
{
    return logicalFunction;
}


std::shared_ptr<DataType> FieldAssignmentLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction FieldAssignmentLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return *this;
};

std::vector<LogicalFunction> FieldAssignmentLogicalFunction::getChildren() const
{
    return {};
};

LogicalFunction FieldAssignmentLogicalFunction::withChildren(std::vector<LogicalFunction>) const
{
    return *this;
};

std::string FieldAssignmentLogicalFunction::getType() const
{
    return std::string(NAME);
}

LogicalFunction FieldAssignmentLogicalFunction::withInferredStamp(Schema schema) const
{
    auto copy = *this;
    /// infer stamp of assignment function
    copy.logicalFunction = logicalFunction.withInferredStamp(schema);

    ///Update the field name with fully qualified field name
    auto fieldName = getField().getFieldName();
    auto existingField = schema.getFieldByName(fieldName);
    if (existingField)
    {
        const auto stamp = copy.logicalFunction.getStamp()->join(*copy.fieldAccess.getStamp());
        copy.fieldAccess = fieldAccess.withFieldName(existingField.value().getName()).withStamp(stamp).get<FieldAccessLogicalFunction>();
    }
    else
    {
        ///Since this is a new field add the source name from schema
        ///Check if field name is already fully qualified
        if (fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos)
        {
            copy.fieldAccess = copy.fieldAccess.withFieldName(fieldName).get<FieldAccessLogicalFunction>();
        }
        else
        {
            copy.fieldAccess = copy.fieldAccess.withFieldName(schema.getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldName)
                                   .get<FieldAccessLogicalFunction>();
        }
    }

    if (!copy.fieldAccess.getStamp() || dynamic_cast<const Undefined*>(copy.fieldAccess.getStamp().get()) != nullptr)
    {
        copy.fieldAccess = copy.fieldAccess.withStamp(copy.getAssignment().getStamp()).get<FieldAccessLogicalFunction>();
    }
    else
    {
        /// the field already has a type, check if it is compatible with the assignment
        if (copy.logicalFunction.getStamp() != copy.fieldAccess.getStamp())
        {
            NES_WARNING(
                "Field {} stamp is incompatible with assignment stamp. Overwriting field stamp with assignment stamp.",
                getField().getFieldName())
            copy.fieldAccess = copy.fieldAccess.withStamp(copy.getAssignment().getStamp()).get<FieldAccessLogicalFunction>();
        }
    }
    copy.stamp = copy.getAssignment().getStamp();
    return copy;
}

SerializableFunction FieldAssignmentLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(fieldAccess.serialize());
    serializedFunction.add_children()->CopyFrom(logicalFunction.serialize());
    DataTypeSerializationUtil::serializeDataType(getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFieldAssignmentLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return FieldAssignmentLogicalFunction(arguments.children[0].get<FieldAccessLogicalFunction>(), arguments.children[1]);
}

}
