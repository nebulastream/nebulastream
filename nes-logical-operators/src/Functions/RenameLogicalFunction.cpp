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
#include <span>
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/RenameLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
RenameLogicalFunction::RenameLogicalFunction(const FieldAccessLogicalFunction& originalField, std::string newFieldName)
    : stamp(originalField.getStamp()), child(originalField), newFieldName(std::move(newFieldName)) {};

RenameLogicalFunction::RenameLogicalFunction(const RenameLogicalFunction& other)
    : RenameLogicalFunction(other.getOriginalField(), other.getNewFieldName()) {};


bool RenameLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const RenameLogicalFunction*>(&rhs))
    {
        return other->child.operator==(getOriginalField()) && this->newFieldName == other->getNewFieldName();
    }
    return false;
}

std::shared_ptr<DataType> RenameLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction RenameLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
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
        return fmt::format("FieldRenameFunction({} => {} : {})", 
            child.explain(verbosity), 
            newFieldName,
            stamp->toString());
    }
    return fmt::format("FieldRename({} => {})", child.explain(verbosity), newFieldName);
}

LogicalFunction RenameLogicalFunction::withInferredStamp(const Schema& schema) const
{
    auto fieldName = child.withInferredStamp(schema).get<FieldAccessLogicalFunction>().getFieldName();
    auto fieldAttribute = schema.getFieldByName(fieldName);
    ///Detect if user has added attribute name separator
    if (!fieldAttribute)
    {
        throw FieldNotFound("Original field with name: {} does not exists in the schema: {}", fieldName, schema.toString());
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
        auto newFieldAttribute = schema.getFieldByName(newFieldName);
        if (newFieldAttribute)
        {
            throw FieldAlreadyExists("New field with name " + newFieldName + " already exists in the schema " + schema.toString());
        }
    }
    /// assign the stamp of this field access with the type of this field.
    auto copy = *this;
    copy.stamp = fieldAttribute.value().getDataType();
    copy.newFieldName = fieldName;
    return copy;
}


SerializableFunction RenameLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());

    NES::Configurations::DescriptorConfig::ConfigType configVariant = getNewFieldName();
    SerializableVariantDescriptor variantDescriptor = Configurations::descriptorConfigTypeToProto(configVariant);
    (*serializedFunction.mutable_config())["NewFieldName"] = variantDescriptor;

    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());

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
