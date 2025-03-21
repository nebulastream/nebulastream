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
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <fmt/format.h>
#include <Common/DataTypes/DataType.hpp>
#include <SerializableFunction.pb.h>
#include <API/Schema.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
ConstantValueLogicalFunction::ConstantValueLogicalFunction(std::shared_ptr<DataType> stamp, std::string value)
    : constantValue(std::move(value)), stamp(stamp) {}

ConstantValueLogicalFunction::ConstantValueLogicalFunction(const ConstantValueLogicalFunction& other)
    :  constantValue(other.constantValue), stamp(other.stamp->clone())
{
}

bool ConstantValueLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const ConstantValueLogicalFunction*>(&rhs);
    if (other)
    {
        return other->stamp == stamp && constantValue == other->constantValue;
    }
    return false;
}

std::string ConstantValueLogicalFunction::toString() const
{
    return fmt::format("ConstantValue({}, {})", constantValue, stamp->toString());
}

std::string ConstantValueLogicalFunction::getConstantValue() const
{
    return constantValue;
}

void ConstantValueLogicalFunction::inferStamp(const Schema&)
{
    /// the stamp of constant value functions is defined by the constant value type.
    /// thus ut is already assigned correctly when the function node is created.
}

SerializableFunction ConstantValueLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    NES::Configurations::DescriptorConfig::ConfigType configVariant = getConstantValue();
    SerializableVariantDescriptor variantDescriptor =
        Configurations::descriptorConfigTypeToProto(configVariant);
    (*serializedFunction.mutable_config())["constantValueAsString"] = variantDescriptor;

    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterConstantValueLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    auto constantValueAsString = get<std::string>(arguments.config["constantValueAsString"]);
    return ConstantValueLogicalFunction(std::move(arguments.stamp), constantValueAsString);
}

}
