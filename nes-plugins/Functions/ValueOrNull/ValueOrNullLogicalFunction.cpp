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

#include "ValueOrNullLogicalFunction.hpp"

#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

ValueOrNullLogicalFunction::ValueOrNullLogicalFunction(LogicalFunction predicate, LogicalFunction value)
    : dataType(value.getDataType()), predicate(std::move(predicate)), value(std::move(value))
{
    dataType.nullable = true;
}

DataType ValueOrNullLogicalFunction::getDataType() const
{
    return dataType;
}

ValueOrNullLogicalFunction ValueOrNullLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction ValueOrNullLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();

    if (not newChildren.at(0).getDataType().isType(DataType::Type::BOOLEAN))
    {
        throw CannotDeserialize(
            "the dataType of the predicate (first argument) of value_or_null must be boolean, but was: {}",
            newChildren[0].getDataType());
    }

    auto newDataType = newChildren.at(1).getDataType();
    newDataType.nullable = true;
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> ValueOrNullLogicalFunction::getChildren() const
{
    return {predicate, value};
}

ValueOrNullLogicalFunction ValueOrNullLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "ValueOrNullLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.predicate = children[0];
    copy.value = children[1];
    return copy;
}

std::string_view ValueOrNullLogicalFunction::getType() const
{
    return NAME;
}

bool ValueOrNullLogicalFunction::operator==(const ValueOrNullLogicalFunction& rhs) const
{
    return predicate == rhs.predicate and value == rhs.value;
}

std::string ValueOrNullLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ValueOrNull({}, {} : {})", predicate.explain(verbosity), value.explain(verbosity), dataType);
    }
    return fmt::format("value_or_null({}, {})", predicate.explain(verbosity), value.explain(verbosity));
}

Reflected Reflector<ValueOrNullLogicalFunction>::operator()(const ValueOrNullLogicalFunction& function) const
{
    return reflect(detail::ReflectedValueOrNullLogicalFunction{.predicate = function.predicate, .value = function.value});
}

ValueOrNullLogicalFunction Unreflector<ValueOrNullLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [predicate, value] = unreflect<detail::ReflectedValueOrNullLogicalFunction>(reflected);
    if (!predicate.has_value() || !value.has_value())
    {
        throw CannotDeserialize("ValueOrNullLogicalFunction is missing a child");
    }
    return ValueOrNullLogicalFunction(predicate.value(), value.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::Registervalue_or_nullLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ValueOrNullLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize(
            "ValueOrNullLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return ValueOrNullLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
