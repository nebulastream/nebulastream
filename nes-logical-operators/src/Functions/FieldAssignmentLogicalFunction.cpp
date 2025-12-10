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

#include <Functions/FieldAssignmentLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{


FieldAssignmentLogicalFunction::FieldAssignmentLogicalFunction(
    FieldAccessLogicalFunction fieldAccess, const LogicalFunction& logicalFunction)
    : dataType(logicalFunction.getDataType()), fieldAccess(std::move(fieldAccess)), logicalFunction(logicalFunction)
{
}

bool FieldAssignmentLogicalFunction::operator==(const FieldAssignmentLogicalFunction& rhs) const
{
    const bool fieldsMatch = this->fieldAccess == rhs.fieldAccess;
    const bool assignmentsMatch = this->logicalFunction == rhs.logicalFunction;
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

DataType FieldAssignmentLogicalFunction::getDataType() const
{
    return dataType;
};

FieldAssignmentLogicalFunction FieldAssignmentLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> FieldAssignmentLogicalFunction::getChildren() const
{
    return {};
};

FieldAssignmentLogicalFunction FieldAssignmentLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
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
        if (const auto dataType = copy.logicalFunction.getDataType().join(copy.fieldAccess.getDataType()))
        {
            copy.fieldAccess = fieldAccess.withFieldName(existingField.value().name).withDataType(dataType.value());
        }
        else
        {
            throw TypeInferenceException("Cannot join {} with {}", copy.logicalFunction.getDataType(), copy.fieldAccess.getDataType());
        }
    }
    else
    {
        ///Since this is a new field add the source name from schema
        ///Check if field name is already fully qualified
        if (fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos)
        {
            copy.fieldAccess = copy.fieldAccess.withFieldName(fieldName);
        }
        else
        {
            copy.fieldAccess = copy.fieldAccess.withFieldName(schema.getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldName);
        }
    }

    if (copy.fieldAccess.getDataType().isType(DataType::Type::UNDEFINED))
    {
        copy.fieldAccess = copy.fieldAccess.withDataType(copy.logicalFunction.getDataType());
    }
    else
    {
        /// the field already has a type, check if it is compatible with the assignment
        if (copy.logicalFunction.getDataType() != copy.fieldAccess.getDataType())
        {
            NES_WARNING(
                "Field {} dataType is incompatible with assignment dataType. Overwriting field dataType with assignment dataType.",
                getField().getFieldName())
            copy.fieldAccess = copy.fieldAccess.withDataType(copy.getAssignment().getDataType());
        }
    }
    copy.dataType = copy.fieldAccess.getDataType();
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
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("FieldAssignmentLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    if (!arguments.children[0].tryGetAs<FieldAccessLogicalFunction>())
    {
        throw CannotDeserialize("First child must be a FieldAccessLogicalFunction");
    }
    return FieldAssignmentLogicalFunction(arguments.children[0].getAs<FieldAccessLogicalFunction>().get(), arguments.children[1]);
}

}
