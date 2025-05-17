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
#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{
FieldAccessLogicalFunction::FieldAccessLogicalFunction(std::string fieldName)
    : fieldName(std::move(fieldName)), dataType(DataTypeProvider::provideDataType(LogicalType::UNDEFINED)) { };

FieldAccessLogicalFunction::FieldAccessLogicalFunction(std::shared_ptr<DataType> dataType, std::string fieldName)
    : fieldName(std::move(fieldName)), dataType(std::move(std::move(dataType))) { };

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
    const bool fieldNamesMatch = rhs.fieldName == lhs.fieldName;
    const bool dataTypesMatch = *rhs.dataType == *lhs.dataType;
    return fieldNamesMatch and dataTypesMatch;
}

bool operator!=(const FieldAccessLogicalFunction& lhs, const FieldAccessLogicalFunction& rhs)
{
    return !(lhs == rhs);
}

std::string FieldAccessLogicalFunction::getFieldName() const
{
    return fieldName;
}

LogicalFunction FieldAccessLogicalFunction::withFieldName(std::string fieldName) const
{
    auto copy = *this;
    copy.fieldName = std::move(fieldName);
    return copy;
}

std::string FieldAccessLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FieldAccessLogicalFunction({}{})", fieldName, dataType ? fmt::format(" [{}]", dataType->toString()) : "");
    }
    return fieldName;
}

LogicalFunction FieldAccessLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto existingField = schema.getFieldByName(fieldName);
    INVARIANT(existingField, "field is not part of the schema");

    auto copy = *this;
    copy.dataType = existingField.value().getDataType();
    copy.fieldName = existingField.value().getName();
    return copy;
}

std::shared_ptr<DataType> FieldAccessLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction FieldAccessLogicalFunction::withDataType(std::shared_ptr<DataType> newStamp) const
{
    PRECONDITION(newStamp != nullptr, "newStamp is null in FieldAccessLogicalFunction::withStamp");
    auto copy = *this;
    copy.dataType = newStamp;
    return copy;
}

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

    const NES::Configurations::DescriptorConfig::ConfigType configVariant = getFieldName();
    const SerializableVariantDescriptor variantDescriptor = descriptorConfigTypeToProto(configVariant);
    (*serializedFunction.mutable_config())["FieldName"] = variantDescriptor;

    DataTypeSerializationUtil::serializeDataType(dataType, serializedFunction.mutable_data_type());

    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFieldAccessLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.config.contains("FieldName"), "FieldAccessLogicalFunction requires a FieldName in its config");
    auto fieldName = get<std::string>(arguments.config["FieldName"]);
    PRECONDITION(!fieldName.empty(), "FieldName cannot be empty");
    return FieldAccessLogicalFunction(arguments.dataType, fieldName);
}

}
