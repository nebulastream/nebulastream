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
#include <Abstract/LogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/format.h>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableFunction.pb.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
ConstantValueLogicalFunction::ConstantValueLogicalFunction(std::shared_ptr<DataType> stamp, std::string value)
    : constantValue(std::move(value)), stamp(stamp)
{
}

ConstantValueLogicalFunction::ConstantValueLogicalFunction(const ConstantValueLogicalFunction& other)
    : constantValue(other.constantValue), stamp(other.stamp->clone())
{
}

const DataType& ConstantValueLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction ConstantValueLogicalFunction::withStamp(std::unique_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp->clone();
    return copy;
};

std::vector<LogicalFunction> ConstantValueLogicalFunction::getChildren() const
{
    return {};
};

LogicalFunction ConstantValueLogicalFunction::withChildren(std::vector<LogicalFunction>) const
{
    return *this;
};

std::string ConstantValueLogicalFunction::getType() const
{
    return std::string(NAME);
}

bool ConstantValueLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const ConstantValueLogicalFunction*>(&rhs))
    {
        return constantValue == other->constantValue;
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

LogicalFunction ConstantValueLogicalFunction::withInferredStamp(Schema) const
{
    /// the stamp of constant value functions is defined by the constant value type.
    /// thus ut is already assigned correctly when the function node is created.
    return *this;
}

SerializableFunction ConstantValueLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);

    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());

    NES::Configurations::DescriptorConfig::ConfigType configVariant = getConstantValue();
    SerializableVariantDescriptor variantDescriptor = Configurations::descriptorConfigTypeToProto(configVariant);
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
