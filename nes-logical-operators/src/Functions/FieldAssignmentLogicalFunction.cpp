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

#include <string>
#include <memory>
#include <sstream>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Undefined.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>

namespace NES
{

FieldAssignmentLogicalFunction::FieldAssignmentLogicalFunction(const FieldAssignmentLogicalFunction& other)
    : stamp(other.stamp), fieldAccess(other.fieldAccess), logicalFunction(other.logicalFunction) {};

FieldAssignmentLogicalFunction::FieldAssignmentLogicalFunction(const FieldAccessLogicalFunction& fieldAccess, LogicalFunction logicalFunction)
    : stamp(logicalFunction.getStamp().clone()), fieldAccess(fieldAccess), logicalFunction(logicalFunction)
{
}

bool FieldAssignmentLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto *other = dynamic_cast<const FieldAssignmentLogicalFunction*>(&rhs))
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

const FieldAccessLogicalFunction& FieldAssignmentLogicalFunction::getField() const
{
    return fieldAccess;
}

LogicalFunction FieldAssignmentLogicalFunction::getAssignment() const
{
    return logicalFunction;
}


const DataType& FieldAssignmentLogicalFunction::getStamp() const
{
return *stamp;
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
    copy.logicalFunction = getAssignment().withInferredStamp(schema);

    ///Update the field name with fully qualified field name
    auto fieldName = getField().getFieldName();
    auto existingField = schema.getFieldByName(fieldName);
    if (existingField)
    {
        const auto stamp = getAssignment().getStamp().join(getField().getStamp());
        copy.fieldAccess = fieldAccess.withFieldName(existingField.value().getName()).get<FieldAccessLogicalFunction>();
    }
    else
    {
        ///Since this is a new field add the source name from schema
        ///Check if field name is already fully qualified
        if (fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos)
        {
            return copy.fieldAccess.withFieldName(fieldName);
        }
        else
        {
            return copy.fieldAccess.withFieldName(schema.getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldName);
        }
    }

    if (dynamic_cast<const Undefined*>(&getField().getStamp()) != nullptr)
    {
        /// if the field has no stamp set it to the one of the assignment
        return copy.fieldAccess.withStamp(getAssignment().getStamp().clone());
    }
    else
    {
        /// the field already has a type, check if it is compatible with the assignment
        if (getField().getStamp() != getAssignment().getStamp())
        {
            NES_WARNING(
                "Field {} stamp is incompatible with assignment stamp. Overwriting field stamp with assignment stamp.",
                getField().getFieldName())
            auto newFieldAccess = fieldAccess.withStamp(getAssignment().getStamp().clone());
        }
    }
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

BinaryLogicalFunctionRegistryReturnType
BinaryLogicalFunctionGeneratedRegistrar::RegisterFieldAssignmentBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return FieldAssignmentLogicalFunction(arguments.leftChild.get<FieldAccessLogicalFunction>(), arguments.rightChild);
}

}
