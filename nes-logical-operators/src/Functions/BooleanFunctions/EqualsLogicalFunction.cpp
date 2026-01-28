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

#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>

#include <algorithm>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
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
EqualsLogicalFunction::EqualsLogicalFunction(LogicalFunction left, LogicalFunction right)
    : left(std::move(left)), right(std::move(right)), dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
{
}

bool EqualsLogicalFunction::operator==(const EqualsLogicalFunction& rhs) const
{
    const bool simpleMatch = left == rhs.left and right == rhs.right;
    const bool commutativeMatch = left == rhs.right and right == rhs.left;
    return simpleMatch or commutativeMatch;
}

DataType EqualsLogicalFunction::getDataType() const
{
    return dataType;
};

EqualsLogicalFunction EqualsLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction EqualsLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    auto newDataType = this->getDataType();
    newDataType.nullable = std::ranges::any_of(newChildren, [](const auto& child) { return child.getDataType().nullable; });
    return withDataType(newDataType).withChildren(newChildren);
};

std::vector<LogicalFunction> EqualsLogicalFunction::getChildren() const
{
    return {left, right};
};

EqualsLogicalFunction EqualsLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "EqualsLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view EqualsLogicalFunction::getType() const
{
    return NAME;
}

std::string EqualsLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} = {}", left.explain(verbosity), right.explain(verbosity));
}

Reflected Reflector<EqualsLogicalFunction>::operator()(const EqualsLogicalFunction& function) const
{
    return reflect(detail::ReflectedEqualsLogicalFunction{
        .left = std::make_optional<LogicalFunction>(function.left), .right = std::make_optional<LogicalFunction>(function.right)});
}

EqualsLogicalFunction Unreflector<EqualsLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [left, right] = unreflect<detail::ReflectedEqualsLogicalFunction>(reflected);

    if (!left.has_value() || !right.has_value())
    {
        throw CannotDeserialize("EqualsLogicalFunction doesn't have a child operator");
    }

    return EqualsLogicalFunction{left.value(), right.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterEqualsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<EqualsLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("EqualsLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return EqualsLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
