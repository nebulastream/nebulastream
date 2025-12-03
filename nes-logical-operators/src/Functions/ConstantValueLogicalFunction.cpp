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

#include <Functions/ConstantValueLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
ConstantValueLogicalFunction::ConstantValueLogicalFunction(DataType dataType, std::string constantValueAsString)
    : constantValue(std::move(constantValueAsString)), dataType(std::move(dataType))
{
}

DataType ConstantValueLogicalFunction::getDataType() const
{
    return dataType;
};

ConstantValueLogicalFunction ConstantValueLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> ConstantValueLogicalFunction::getChildren() const
{
    return {};
};

ConstantValueLogicalFunction ConstantValueLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
};

std::string_view ConstantValueLogicalFunction::getType() const
{
    return NAME;
}

bool ConstantValueLogicalFunction::operator==(const ConstantValueLogicalFunction& rhs) const
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
        return fmt::format("ConstantValueLogicalFunction({} : {})", constantValue, dataType);
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

Reflected Reflector<ConstantValueLogicalFunction>::operator()(const ConstantValueLogicalFunction& function) const
{
    return reflect(
        detail::ReflectedConstantValueLogicalFunction{.value = function.getConstantValue(), .type = function.getDataType().type});
}

ConstantValueLogicalFunction Unreflector<ConstantValueLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [value, type] = unreflect<detail::ReflectedConstantValueLogicalFunction>(reflected);
    return ConstantValueLogicalFunction{DataType{type}, value};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterConstantValueLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ConstantValueLogicalFunction>(arguments.reflected);
    }

    if (not arguments.config.contains("constantValueAsString"))
    {
        throw CannotDeserialize("ConstantValueLogicalFunction requires a constantValueAsString in its config");
    }
    auto constantValueAsString = get<std::string>(arguments.config["constantValueAsString"]);
    return ConstantValueLogicalFunction(std::move(arguments.dataType), constantValueAsString);
}

}
