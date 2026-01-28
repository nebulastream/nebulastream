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

#include <Functions/ConcatLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
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

ConcatLogicalFunction::ConcatLogicalFunction(const LogicalFunction& left, const LogicalFunction& right)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), left(left), right(right)
{
}

bool ConcatLogicalFunction::operator==(const ConcatLogicalFunction& rhs) const
{
    const bool simpleMatch = left == rhs.left and right == rhs.right;
    const bool commutativeMatch = left == rhs.right and right == rhs.left;
    return simpleMatch or commutativeMatch;
}

std::string ConcatLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("CONCAT({}, {})", left.explain(verbosity), right.explain(verbosity));
}

DataType ConcatLogicalFunction::getDataType() const
{
    return dataType;
};

ConcatLogicalFunction ConcatLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction ConcatLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 2, "ConcatLogicalFunction expects exactly two child function but has {}", newChildren.size());
    auto newDataType = newChildren[0].getDataType().join(newChildren[1].getDataType());
    if (not newDataType.has_value() or not newDataType.value().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected(
            "Expected VARSIZED as outcome of {} and {}", newChildren[0].getDataType(), newChildren[1].getDataType());
    }
    newDataType.value().nullable = std::ranges::any_of(newChildren, [](const auto& child) { return child.getDataType().nullable; });
    return withDataType(newDataType.value()).withChildren(newChildren);
};

std::vector<LogicalFunction> ConcatLogicalFunction::getChildren() const
{
    return {left, right};
};

ConcatLogicalFunction ConcatLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.dataType
        = children[0].getDataType().join(children[1].getDataType()).value_or(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED));
    return copy;
};

std::string_view ConcatLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<ConcatLogicalFunction>::operator()(const ConcatLogicalFunction& function) const
{
    return reflect(detail::ReflectedConcatLogicalFunction{.left = function.left, .right = function.right});
}

ConcatLogicalFunction Unreflector<ConcatLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [left, right] = unreflect<detail::ReflectedConcatLogicalFunction>(reflected);

    if (!left.has_value() || !right.has_value())
    {
        throw CannotDeserialize("ConcatLogicalFunction is missing a child");
    }
    return ConcatLogicalFunction{left.value(), right.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterConcatLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ConcatLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() < 2)
    {
        throw CannotDeserialize("ConcatLogicalFunction requires two children, but only got {}", arguments.children.size());
    }
    return ConcatLogicalFunction(*(arguments.children.end() - 2), *(arguments.children.end() - 1));
}

}
