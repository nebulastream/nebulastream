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
#include <API/Schema.hpp>
#include <LogicalFunctions/Function.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/format.h>
#include <FunctionRegistry.hpp>
#include <SerializableFunction.pb.h>
#include <Common/DataTypes/DataType.hpp>
#include <sstream>
#include <LogicalFunctions/ConstantValueFunction.hpp>

namespace NES::Logical
{
ConstantValueFunction::ConstantValueFunction(std::shared_ptr<DataType> stamp, std::string value)
    : constantValue(std::move(value)), stamp(stamp)
{
}

ConstantValueFunction::ConstantValueFunction(const ConstantValueFunction& other)
    : constantValue(other.constantValue), stamp(other.stamp)
{
}

std::shared_ptr<DataType> ConstantValueFunction::getStamp() const
{
    return stamp;
};

Function ConstantValueFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

std::vector<Function> ConstantValueFunction::getChildren() const
{
    return {};
};

Function ConstantValueFunction::withChildren(const std::vector<Function>&) const
{
    return *this;
};

std::string_view ConstantValueFunction::getType() const
{
    return NAME;
}

bool ConstantValueFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const ConstantValueFunction*>(&rhs))
    {
        return constantValue == other->constantValue;
    }
    return false;
}

std::string ConstantValueFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ConstantValueFunction({} : {})", 
            constantValue,
            stamp->toString());
    }
    return constantValue;
}

std::string ConstantValueFunction::getConstantValue() const
{
    return constantValue;
}

Function ConstantValueFunction::withInferredStamp(const Schema&) const
{
    /// the stamp of constant value functions is defined by the constant value type.
    /// thus it is already assigned correctly when the function node is created.
    return *this;
}

SerializableFunction ConstantValueFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);

    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());

    NES::Configurations::DescriptorConfig::ConfigType configVariant = getConstantValue();
    SerializableVariantDescriptor variantDescriptor = Configurations::descriptorConfigTypeToProto(configVariant);
    (*serializedFunction.mutable_config())["constantValueAsString"] = variantDescriptor;

    return serializedFunction;
}

FunctionRegistryReturnType
FunctionGeneratedRegistrar::RegisterConstantValueFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.config.contains("constantValueAsString"), "ConstantValueFunction requires a constantValueAsString in its config");
    auto constantValueAsString = get<std::string>(arguments.config["constantValueAsString"]);
    return ConstantValueFunction(std::move(arguments.stamp), constantValueAsString);
}

}
