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

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>

#include <algorithm>
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

AndLogicalFunction::AndLogicalFunction(LogicalFunction left, LogicalFunction right)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN)), left(std::move(left)), right(std::move(right))
{
}

DataType AndLogicalFunction::getDataType() const
{
    return dataType;
};

AndLogicalFunction AndLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> AndLogicalFunction::getChildren() const
{
    return {left, right};
};

AndLogicalFunction AndLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "AndLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view AndLogicalFunction::getType() const
{
    return NAME;
}

bool AndLogicalFunction::operator==(const AndLogicalFunction& rhs) const
{
    const bool simpleMatch = left == rhs.left and right == rhs.right;
    const bool commutativeMatch = left == rhs.right and right == rhs.left;
    return simpleMatch or commutativeMatch;
}

std::string AndLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} AND {}", left.explain(verbosity), right.explain(verbosity));
}

LogicalFunction AndLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();

    /// check if children dataType is correct
    if (not newChildren.at(0).getDataType().isType(DataType::Type::BOOLEAN))
    {
        throw CannotDeserialize("the dataType of left child must be boolean, but was: {}", newChildren[0].getDataType());
    }
    if (not newChildren.at(1).getDataType().isType(DataType::Type::BOOLEAN))
    {
        throw CannotDeserialize("the dataType of right child must be boolean, but was: {}", newChildren[1].getDataType());
    }

    auto newDataType = this->getDataType();
    newDataType.nullable = std::ranges::any_of(newChildren, [](const auto& child) { return child.getDataType().nullable; });
    return withDataType(newDataType).withChildren(newChildren);
}

Reflected Reflector<AndLogicalFunction>::operator()(const AndLogicalFunction& function) const
{
    return reflect(detail::ReflectedAndLogicalFunction{.left = function.left, .right = function.right});
}

AndLogicalFunction Unreflector<AndLogicalFunction>::operator()(const Reflected& rfl) const
{
    auto [left, right] = unreflect<detail::ReflectedAndLogicalFunction>(rfl);

    if (!left.has_value() || !right.has_value())
    {
        throw CannotDeserialize("AndLogicalFunction is missing a child");
    }
    return {left.value(), right.value()};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAndLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<AndLogicalFunction>(arguments.reflected);
    }

    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("AndLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return AndLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
