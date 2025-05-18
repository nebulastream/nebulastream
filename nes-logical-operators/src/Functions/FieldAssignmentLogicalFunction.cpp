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
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Undefined.hpp>

namespace NES
{


FieldAssignmentLogicalFunction::FieldAssignmentLogicalFunction(
    FieldAccessLogicalFunction fieldAccess, const LogicalFunction& logicalFunction)
    : dataType(logicalFunction.getDataType()), fieldAccess(std::move(fieldAccess)), logicalFunction(logicalFunction)
{
}

bool FieldAssignmentLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const FieldAssignmentLogicalFunction*>(&rhs))
    {
        return *this == *other;
    }
    return false;
}

bool operator==(const FieldAssignmentLogicalFunction& lhs, const FieldAssignmentLogicalFunction& rhs)
{
    /// a field assignment function has always two children.
    const bool fieldsMatch = lhs.fieldAccess == rhs.fieldAccess;
    const bool assignmentsMatch = lhs.logicalFunction == rhs.logicalFunction;
    return fieldsMatch and assignmentsMatch;
}

bool operator!=(const FieldAssignmentLogicalFunction& lhs, const FieldAssignmentLogicalFunction& rhs)
{
    return !(lhs == rhs);
}

std::string FieldAssignmentLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FieldAssignmentLogicalFunction({} = {})", fieldAccess.explain(verbosity), logicalFunction.explain(verbosity));
    }
    return fmt::format("{} = {}", fieldAccess.explain(verbosity), logicalFunction.explain(verbosity));
}

FieldAccessLogicalFunction FieldAssignmentLogicalFunction::getField() const
{
    return fieldAccess;
}

LogicalFunction FieldAssignmentLogicalFunction::getAssignment() const
{
    return logicalFunction;
}


std::shared_ptr<DataType> FieldAssignmentLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction FieldAssignmentLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> FieldAssignmentLogicalFunction::getChildren() const
{
    return {};
};

LogicalFunction FieldAssignmentLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
};

std::string_view FieldAssignmentLogicalFunction::getType() const
{
    return NAME;
}

LogicalFunction FieldAssignmentLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto copy = *this;
    /// infer dataType of assignment function
    copy.logicalFunction = logicalFunction.withInferredDataType(schema);

    ///Update the field name with fully qualified field name
    auto fieldName = getField().getFieldName();
    auto existingField = schema.getFieldByName(fieldName);
    if (existingField)
    {
        const auto dataType = copy.logicalFunction.getDataType()->join(*copy.fieldAccess.getDataType());
        copy.fieldAccess
            = fieldAccess.withFieldName(existingField.value().getName()).withDataType(dataType).get<FieldAccessLogicalFunction>();
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

    if (!copy.fieldAccess.getDataType() || (dynamic_cast<const Undefined*>(copy.fieldAccess.getDataType().get()) != nullptr))
    {
        copy.fieldAccess = copy.fieldAccess.withDataType(copy.getAssignment().getDataType()).get<FieldAccessLogicalFunction>();
    }
    else
    {
        /// the field already has a type, check if it is compatible with the assignment
        if (copy.logicalFunction.getDataType() != copy.fieldAccess.getDataType())
        {
            NES_WARNING(
                "Field {} dataType is incompatible with assignment dataType. Overwriting field dataType with assignment dataType.",
                getField().getFieldName())
            copy.fieldAccess = copy.fieldAccess.withDataType(copy.getAssignment().getDataType()).get<FieldAccessLogicalFunction>();
        }
    }
    copy.dataType = copy.getAssignment().getDataType();
    return copy;
}

SerializableFunction FieldAssignmentLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(fieldAccess.serialize());
    serializedFunction.add_children()->CopyFrom(logicalFunction.serialize());
    DataTypeSerializationUtil::serializeDataType(getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFieldAssignmentLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.children.size() == 2,
        "FieldAssignmentLogicalFunction requires exactly two children, but got {}",
        arguments.children.size());
    PRECONDITION(arguments.children[0].tryGet<FieldAccessLogicalFunction>(), "First child must be a FieldAccessLogicalFunction");
    return FieldAssignmentLogicalFunction(arguments.children[0].get<FieldAccessLogicalFunction>(), arguments.children[1]);
}

}
