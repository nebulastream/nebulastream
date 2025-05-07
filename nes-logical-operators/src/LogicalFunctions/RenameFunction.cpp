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
#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FunctionRegistry.hpp>
#include <LogicalFunctions/RenameFunction.hpp>

namespace NES::Logical
{
RenameFunction::RenameFunction(const FieldAccessFunction& originalField, std::string newFieldName)
    : stamp(originalField.getStamp()), child(originalField), newFieldName(std::move(newFieldName)) {};

RenameFunction::RenameFunction(const RenameFunction& other)
    : RenameFunction(other.getOriginalField(), other.getNewFieldName()) {};


bool RenameFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const RenameFunction*>(&rhs))
    {
        return other->child.operator==(getOriginalField()) && this->newFieldName == other->getNewFieldName();
    }
    return false;
}

std::shared_ptr<DataType> RenameFunction::getStamp() const
{
    return stamp;
};

Function RenameFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

std::vector<Function> RenameFunction::getChildren() const
{
    return {child};
};

Function RenameFunction::withChildren(const std::vector<Function>& children) const
{
    auto copy = *this;
    copy.child = children[0].get<FieldAccessFunction>();
    return copy;
};

std::string_view RenameFunction::getType() const
{
    return NAME;
}

const FieldAccessFunction& RenameFunction::getOriginalField() const
{
    return child;
}

std::string RenameFunction::getNewFieldName() const
{
    return newFieldName;
}

std::string RenameFunction::explain(ExplainVerbosity verbosity) const
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

Function RenameFunction::withInferredStamp(const Schema& schema) const
{
    auto fieldName = child.withInferredStamp(schema).get<FieldAccessFunction>().getFieldName();
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


SerializableFunction RenameFunction::serialize() const
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


FunctionRegistryReturnType
FunctionGeneratedRegistrar::RegisterRenameFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.config.contains("NewFieldName"), "RenameFunction requires a NewFieldName in its config");
    PRECONDITION(arguments.children.size() == 1, "RenameFunction requires exactly one child, but got {}", arguments.children.size());
    PRECONDITION(arguments.children[0].tryGet<FieldAccessFunction>(), "Child must be a FieldAccessFunction");
    auto newFieldName = get<std::string>(arguments.config["NewFieldName"]);
    PRECONDITION(!newFieldName.empty(), "NewFieldName cannot be empty");
    return RenameFunction(arguments.children[0].get<FieldAccessFunction>(), newFieldName);
}

}
