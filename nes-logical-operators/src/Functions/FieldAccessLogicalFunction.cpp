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
#include <format>
#include <memory>
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{
FieldAccessLogicalFunction::FieldAccessLogicalFunction(std::string fieldName)
    : fieldName(std::move(fieldName)), stamp(DataTypeProvider::provideDataType(LogicalType::UNDEFINED)) {};

FieldAccessLogicalFunction::FieldAccessLogicalFunction(std::shared_ptr<DataType> stamp, std::string fieldName)
    : fieldName(std::move(fieldName)), stamp(stamp) {};

FieldAccessLogicalFunction::FieldAccessLogicalFunction(const FieldAccessLogicalFunction& other)
    : fieldName(other.fieldName), stamp(other.stamp) {};

bool FieldAccessLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const FieldAccessLogicalFunction*>(&rhs);
    if (other)
    {
        bool fieldNamesMatch = other->fieldName == fieldName;
        const bool stampsMatch = *other->stamp == *stamp;
        return fieldNamesMatch and stampsMatch;
    }
    return false;
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

std::string FieldAccessLogicalFunction::toString() const
{
    return std::format("FieldAccessLogicalFunction( {} [ {} ])", fieldName, stamp->toString());
}

LogicalFunction FieldAccessLogicalFunction::withInferredStamp(Schema schema) const
{
    const auto existingField = schema.getFieldByName(fieldName);
    INVARIANT(existingField, "field is not part of the schema");

    auto copy = *this;
    copy.stamp = existingField.value().getDataType();
    copy.fieldName = existingField.value().getName();
    return copy;
}

std::shared_ptr<DataType> FieldAccessLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction FieldAccessLogicalFunction::withStamp(std::shared_ptr<DataType> newStamp) const
{
    PRECONDITION(newStamp != nullptr, "newStamp is null in FieldAccessLogicalFunction::withStamp");
    auto copy = *this;
    copy.stamp = newStamp;
    return copy;
}

std::vector<LogicalFunction> FieldAccessLogicalFunction::getChildren() const
{
    return {};
};

LogicalFunction FieldAccessLogicalFunction::withChildren(std::vector<LogicalFunction>) const
{
    return *this;
};

std::string FieldAccessLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction FieldAccessLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);

    NES::Configurations::DescriptorConfig::ConfigType configVariant = getFieldName();
    SerializableVariantDescriptor variantDescriptor = Configurations::descriptorConfigTypeToProto(configVariant);
    (*serializedFunction.mutable_config())["FieldName"] = variantDescriptor;

    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFieldAccessLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    auto fieldName = get<std::string>(arguments.config["FieldName"]);
    return FieldAccessLogicalFunction(arguments.stamp, fieldName);
}

}
