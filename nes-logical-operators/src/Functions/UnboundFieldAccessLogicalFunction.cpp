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

#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>

#include <folly/Hash.h>

#include <Schema/Schema.hpp>
#include <Util/Overloaded.hpp>
#include <LogicalFunctionRegistry.hpp>
#include "DataTypes/DataType.hpp"
#include "Identifiers/Identifier.hpp"

namespace NES
{

UnboundFieldAccessLogicalFunction::UnboundFieldAccessLogicalFunction(Identifier fieldName)
    : fieldName(std::move(fieldName)), dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)) { };

UnboundFieldAccessLogicalFunction::UnboundFieldAccessLogicalFunction(Identifier fieldName, DataType dataType)
    : fieldName(std::move(fieldName)), dataType(std::move(std::move(dataType))) { };

bool UnboundFieldAccessLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const UnboundFieldAccessLogicalFunction*>(&rhs))
    {
        return *this == *other;
    }
    return false;
}

bool operator==(const UnboundFieldAccessLogicalFunction& lhs, const UnboundFieldAccessLogicalFunction& rhs)
{
    return lhs.fieldName == rhs.fieldName;
}

bool operator!=(const UnboundFieldAccessLogicalFunction& lhs, const UnboundFieldAccessLogicalFunction& rhs)
{
    return !(lhs == rhs);
}

Identifier UnboundFieldAccessLogicalFunction::getFieldName() const
{
    return fieldName;
}

LogicalFunction UnboundFieldAccessLogicalFunction::withFieldName(Identifier field) const
{
    auto copy = *this;
    copy.fieldName = std::move(field);
    return copy;
}

std::string UnboundFieldAccessLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("UnboundFieldAccessLogicalFunction({})", fieldName);
    }
    return fmt::format("{}", fieldName);
}

LogicalFunction UnboundFieldAccessLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto existingField = schema.getFieldByName(fieldName);
    if (!existingField)
    {
        throw CannotInferSchema("field {} is not part of the schema {}", fieldName, schema);
    }
    return FieldAccessLogicalFunction(existingField.value());
}

DataType UnboundFieldAccessLogicalFunction::getDataType() const
{
    return dataType;
};

std::vector<LogicalFunction> UnboundFieldAccessLogicalFunction::getChildren() const
{
    return {};
};

LogicalFunction UnboundFieldAccessLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
};

std::string_view UnboundFieldAccessLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction UnboundFieldAccessLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);

    const DescriptorConfig::ConfigType configVariant = getFieldName();
    const SerializableVariantDescriptor variantDescriptor = descriptorConfigTypeToProto(configVariant);
    (*serializedFunction.mutable_config())[ConfigParameters::FIELD_NAME] = variantDescriptor;

    DataTypeSerializationUtil::serializeDataType(dataType, serializedFunction.mutable_data_type());

    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterUnboundFieldAccessLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.config.contains(UnboundFieldAccessLogicalFunction::ConfigParameters::FIELD_NAME),
        "FieldAccessLogicalFunction requires a FieldName in its config");
    auto fieldName = get<Identifier>(arguments.config[UnboundFieldAccessLogicalFunction::ConfigParameters::FIELD_NAME]);
    return UnboundFieldAccessLogicalFunction(std::move(fieldName), arguments.dataType);
}

FieldAccessLogicalFunction FieldAccessLogicalFunctionVariantWrapper::withInferredDataType(const Schema& schema) const
{
    return std::visit(
        Overloaded{
            [&schema](const UnboundFieldAccessLogicalFunction& unboundFieldAccessLogicalFunction)
            {
                const auto shouldBeFieldAccess = unboundFieldAccessLogicalFunction.withInferredDataType(schema);
                INVARIANT(
                    shouldBeFieldAccess.tryGet<FieldAccessLogicalFunction>().has_value(),
                    "UnboundFieldAccessLogicalFunction needs return FieldAccessLogicalFunction after schema inferrence");
                return shouldBeFieldAccess.get<FieldAccessLogicalFunction>();
            },
            [&schema](const FieldAccessLogicalFunction& fieldAccessLogicalFunction)
            { return fieldAccessLogicalFunction.withInferredDataType(schema).get<FieldAccessLogicalFunction>(); },
        },
        underlying);
}
}

std::size_t
std::hash<NES::UnboundFieldAccessLogicalFunction>::operator()(const NES::UnboundFieldAccessLogicalFunction& function) const noexcept
{
    return folly::hash::hash_combine(function.fieldName, function.dataType);
}

std::size_t std::hash<NES::FieldAccessLogicalFunctionVariantWrapper>::operator()(
    const NES::FieldAccessLogicalFunctionVariantWrapper& function) const noexcept
{
    return std::hash<std::variant<NES::UnboundFieldAccessLogicalFunction, NES::FieldAccessLogicalFunction>>{}(function.underlying);
}
