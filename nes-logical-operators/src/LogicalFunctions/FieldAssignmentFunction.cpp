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
#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <LogicalFunctions/Function.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <FunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Undefined.hpp>
#include <fmt/format.h>
#include <LogicalFunctions/FieldAssignmentFunction.hpp>

namespace NES::Logical
{

FieldAssignmentFunction::FieldAssignmentFunction(
    const FieldAccessFunction& fieldAccess, Function logicalFunction)
    : stamp(logicalFunction.getStamp()), fieldAccess(fieldAccess), logicalFunction(logicalFunction)
{
}

bool FieldAssignmentFunction::operator==(const FunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const FieldAssignmentFunction*>(&rhs))
    {
        return *this == *other;
    }
    return false;
}

bool operator==(const FieldAssignmentFunction& lhs, const FieldAssignmentFunction& rhs)
{
    /// a field assignment function has always two children.
    const bool fieldsMatch = lhs.fieldAccess == rhs.fieldAccess;
    const bool assignmentsMatch = lhs.logicalFunction == rhs.logicalFunction;
    return fieldsMatch and assignmentsMatch;
}

bool operator!=(const FieldAssignmentFunction& lhs, const FieldAssignmentFunction& rhs)
{
    return !(lhs == rhs);
}

std::string FieldAssignmentFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FieldAssignmentFunction({} = {})", 
            fieldAccess.explain(verbosity), 
            logicalFunction.explain(verbosity));
    }
    return fmt::format("{} = {}", fieldAccess.explain(verbosity), logicalFunction.explain(verbosity));
}

FieldAccessFunction FieldAssignmentFunction::getField() const
{
    return fieldAccess;
}

Function FieldAssignmentFunction::getAssignment() const
{
    return logicalFunction;
}


std::shared_ptr<DataType> FieldAssignmentFunction::getStamp() const
{
    return stamp;
};

Function FieldAssignmentFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

std::vector<Function> FieldAssignmentFunction::getChildren() const
{
    return {};
};

Function FieldAssignmentFunction::withChildren(const std::vector<Function>&) const
{
    return *this;
};

std::string_view FieldAssignmentFunction::getType() const
{
    return NAME;
}

Function FieldAssignmentFunction::withInferredStamp(const Schema& schema) const
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
        copy.fieldAccess = fieldAccess.withFieldName(existingField.value().getName()).withStamp(stamp).get<FieldAccessFunction>();
    }
    else
    {
        ///Since this is a new field add the source name from schema
        ///Check if field name is already fully qualified
        if (fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos)
        {
            copy.fieldAccess = copy.fieldAccess.withFieldName(fieldName).get<FieldAccessFunction>();
        }
        else
        {
            copy.fieldAccess = copy.fieldAccess.withFieldName(schema.getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldName)
                                   .get<FieldAccessFunction>();
        }
    }

    if (!copy.fieldAccess.getStamp() || dynamic_cast<const Undefined*>(copy.fieldAccess.getStamp().get()) != nullptr)
    {
        copy.fieldAccess = copy.fieldAccess.withStamp(copy.getAssignment().getStamp()).get<FieldAccessFunction>();
    }
    else
    {
        /// the field already has a type, check if it is compatible with the assignment
        if (copy.logicalFunction.getStamp() != copy.fieldAccess.getStamp())
        {
            NES_WARNING(
                "Field {} stamp is incompatible with assignment stamp. Overwriting field stamp with assignment stamp.",
                getField().getFieldName())
            copy.fieldAccess = copy.fieldAccess.withStamp(copy.getAssignment().getStamp()).get<FieldAccessFunction>();
        }
    }
    copy.stamp = copy.getAssignment().getStamp();
    return copy;
}

SerializableFunction FieldAssignmentFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(fieldAccess.serialize());
    serializedFunction.add_children()->CopyFrom(logicalFunction.serialize());
    DataTypeSerializationUtil::serializeDataType(getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType
FunctionGeneratedRegistrar::RegisterFieldAssignmentFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "FieldAssignmentFunction requires exactly two children, but got {}", arguments.children.size());
    PRECONDITION(arguments.children[0].tryGet<FieldAccessFunction>(), "First child must be a FieldAccessFunction");
    return FieldAssignmentFunction(arguments.children[0].get<FieldAccessFunction>(), arguments.children[1]);
}

}
