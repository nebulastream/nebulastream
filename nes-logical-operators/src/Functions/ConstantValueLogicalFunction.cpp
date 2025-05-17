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
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
ConstantValueLogicalFunction::ConstantValueLogicalFunction(std::shared_ptr<DataType> dataType, std::string value)
    : constantValue(std::move(value)), dataType(std::move(std::move(dataType)))
{
}

ConstantValueLogicalFunction::ConstantValueLogicalFunction(const ConstantValueLogicalFunction& other)
    : constantValue(other.constantValue), dataType(other.dataType)
{
}

std::shared_ptr<DataType> ConstantValueLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction ConstantValueLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> ConstantValueLogicalFunction::getChildren() const
{
    return {};
};

LogicalFunction ConstantValueLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
};

std::string_view ConstantValueLogicalFunction::getType() const
{
    return NAME;
}

bool ConstantValueLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const ConstantValueLogicalFunction*>(&rhs))
    {
        return constantValue == other->constantValue;
    }
    return false;
}

std::string ConstantValueLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ConstantValueLogicalFunction({} : {})", constantValue, dataType->toString());
    }
    return constantValue;
}

std::string ConstantValueLogicalFunction::getConstantValue() const
{
    return constantValue;
}

LogicalFunction ConstantValueLogicalFunction::withInferredDataType(const Schema&) const
{
    /// the dataType of constant value functions is defined by the constant value type.
    /// thus it is already assigned correctly when the function node is created.
    return *this;
}

SerializableFunction ConstantValueLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);

    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());

    const NES::Configurations::DescriptorConfig::ConfigType configVariant = getConstantValue();
    const SerializableVariantDescriptor variantDescriptor = Configurations::descriptorConfigTypeToProto(configVariant);
    (*serializedFunction.mutable_config())["constantValueAsString"] = variantDescriptor;

    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterConstantValueLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.config.contains("constantValueAsString"), "ConstantValueLogicalFunction requires a constantValueAsString in its config");
    auto constantValueAsString = get<std::string>(arguments.config["constantValueAsString"]);
    return ConstantValueLogicalFunction(std::move(arguments.dataType), constantValueAsString);
}

}
