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

#include <Functions/FieldAccessLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include "Operators/LogicalOperator.hpp"

namespace NES
{
FieldAccessLogicalFunction::FieldAccessLogicalFunction(Field field) : field(std::move(field)) { };

bool FieldAccessLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const FieldAccessLogicalFunction*>(&rhs))
    {
        return *this == *other;
    }
    return false;
}

bool operator==(const FieldAccessLogicalFunction& lhs, const FieldAccessLogicalFunction& rhs)
{
    return lhs.field == rhs.field;
}

bool operator!=(const FieldAccessLogicalFunction& lhs, const FieldAccessLogicalFunction& rhs)
{
    return !(lhs == rhs);
}

Field FieldAccessLogicalFunction::getField() const
{
    return field;
}

LogicalFunction FieldAccessLogicalFunction::withField(Field field) const
{
    auto copy = *this;
    copy.field = std::move(field);
    return copy;
}

std::string FieldAccessLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FieldAccessLogicalFunction({})", field);
    }
    return fmt::format("{}", field);
}

LogicalFunction FieldAccessLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newField = schema.getFieldByName(field.getLastName());
    if (!newField)
    {
        throw CannotInferSchema("field {} is not part of the schema {}", field.getLastName(), schema);
    }

    auto copy = *this;
    copy.field = newField.value();
    return copy;
}

DataType FieldAccessLogicalFunction::getDataType() const
{
    return field.getDataType();
};

std::vector<LogicalFunction> FieldAccessLogicalFunction::getChildren() const
{
    return {};
};

LogicalFunction FieldAccessLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
};

std::string_view FieldAccessLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction FieldAccessLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);

    const DescriptorConfig::ConfigType configVariant = field.getLastName();
    const SerializableVariantDescriptor fieldNameVariant = descriptorConfigTypeToProto(configVariant);
    (*serializedFunction.mutable_config())[ConfigParameters::FIELD_NAME] = fieldNameVariant;
    const SerializableVariantDescriptor fromOperatorVariant
        = descriptorConfigTypeToProto(DescriptorConfig::ConfigType{field.getProducedBy().getId().getRawValue()});
    (*serializedFunction.mutable_config())[ConfigParameters::FROM_OPERATOR] = fromOperatorVariant;

    DataTypeSerializationUtil::serializeDataType(field.getDataType(), serializedFunction.mutable_data_type());

    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFieldAccessLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    const auto& fieldNameIter = arguments.config.find(std::string{FieldAccessLogicalFunction::ConfigParameters::FIELD_NAME});
    if (fieldNameIter == arguments.config.end())
    {
        throw CannotDeserialize("FieldAccessLogicalFunction requires a FieldName in its config");
    }
    const auto fieldName = std::get<Identifier>(fieldNameIter->second);

    const auto& operatorIdIter = arguments.config.find(std::string{FieldAccessLogicalFunction::ConfigParameters::FROM_OPERATOR});
    if (operatorIdIter == arguments.config.end() || !std::holds_alternative<uint64_t>(operatorIdIter->second))
    {
        throw CannotDeserialize("FieldAccessLogicalFunction requires an operator id in its config");
    }
    const auto field = arguments.schema.getFieldByName(fieldName);
    if (!field)
    {
        throw CannotDeserialize("FieldAccessLogicalFunction requires a FieldName that is part of the schema");
    }

    /// TODO enable this check when we removed operator ids from the model and made it serialization only
    /// const uint64_t foundOperatorId = std::get<uint64_t>(operatorIdIter->second);
    /// if (field.value().getProducedBy().getId().getRawValue() != foundOperatorId)
    /// {
    ///     throw CannotDeserialize("Operator id in config does not match the operator id of the field of FieldAccessLogicalFunction");
    /// }
    return FieldAccessLogicalFunction(field.value());
}

}

std::size_t std::hash<NES::FieldAccessLogicalFunction>::operator()(const NES::FieldAccessLogicalFunction& fieldAccessFunction) const noexcept
{
    return std::hash<NES::Field>()(fieldAccessFunction.getField());
}