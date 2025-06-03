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
#include <string_view>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/RenameLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
RenameLogicalFunction::RenameLogicalFunction(const FieldAccessLogicalFunction& originalField, std::string newFieldName)
    : dataType(originalField.getDataType()), child(originalField), newFieldName(std::move(newFieldName)) { };

bool RenameLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const RenameLogicalFunction*>(&rhs))
    {
        return other->child.operator==(getOriginalField()) && this->newFieldName == other->getNewFieldName();
    }
    return false;
}

DataType RenameLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction RenameLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> RenameLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction RenameLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.child = children[0].get<FieldAccessLogicalFunction>();
    return copy;
};

std::string_view RenameLogicalFunction::getType() const
{
    return NAME;
}

const FieldAccessLogicalFunction& RenameLogicalFunction::getOriginalField() const
{
    return child;
}

std::string RenameLogicalFunction::getNewFieldName() const
{
    return newFieldName;
}

std::string RenameLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FieldRenameFunction({} => {} : {})", child.explain(verbosity), newFieldName, dataType);
    }
    return fmt::format("FieldRename({} => {})", child.explain(verbosity), newFieldName);
}

LogicalFunction RenameLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto fieldName = child.withInferredDataType(schema).get<FieldAccessLogicalFunction>().getFieldName();
    auto fieldAttribute = schema.getFieldByName(fieldName);
    ///Detect if user has added attribute name separator
    if (!fieldAttribute)
    {
        throw FieldNotFound("Original field with name: {} does not exists in the schema: {}", fieldName, schema);
    }
    if (newFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        fieldName = fieldName.substr(0, fieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1) + newFieldName;
    }

    if (fieldName == newFieldName)
    {
        NES_WARNING("Both existing and new fields are same: existing: {} new field name: {}", fieldName, newFieldName);
    }
    else
    {
        if (auto newFieldAttribute = schema.getFieldByName(newFieldName))
        {
            throw FieldAlreadyExists("New field with name {} already exists in the schema: {}", newFieldName, schema);
        }
    }
    /// assign the dataType of this field access with the type of this field.
    auto copy = *this;
    copy.dataType = fieldAttribute.value().dataType;
    copy.newFieldName = fieldName;
    return copy;
}


SerializableFunction RenameLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());

    const NES::Configurations::DescriptorConfig::ConfigType configVariant = getNewFieldName();
    const SerializableVariantDescriptor variantDescriptor = Configurations::descriptorConfigTypeToProto(configVariant);
    (*serializedFunction.mutable_config())["NewFieldName"] = variantDescriptor;

    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());

    return serializedFunction;
}


LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterRenameLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.config.contains("NewFieldName"), "RenameLogicalFunction requires a NewFieldName in its config");
    PRECONDITION(arguments.children.size() == 1, "RenameLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    PRECONDITION(arguments.children[0].tryGet<FieldAccessLogicalFunction>(), "Child must be a FieldAccessLogicalFunction");
    auto newFieldName = get<std::string>(arguments.config["NewFieldName"]);
    PRECONDITION(!newFieldName.empty(), "NewFieldName cannot be empty");
    return RenameLogicalFunction(arguments.children[0].get<FieldAccessLogicalFunction>(), newFieldName);
}

}
