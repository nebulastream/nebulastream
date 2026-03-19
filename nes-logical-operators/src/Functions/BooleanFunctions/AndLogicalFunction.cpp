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
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

AndLogicalFunction::AndLogicalFunction(LogicalFunction left, LogicalFunction right)
    : left(std::move(std::move(left))), right(std::move(std::move(right)))
{
}

DataType AndLogicalFunction::getDataType() const
{
    return dataType;
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

LogicalFunction AndLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.left = left.withInferredDataType(schema);
    copy.right = right.withInferredDataType(schema);

    if (!copy.left.getDataType().isType(DataType::Type::BOOLEAN) && copy.right.getDataType().isType(DataType::Type::BOOLEAN))
    {
        throw CannotInferStamp("Can only apply and to two boolean input function, but got left: {}, right: {}", copy.left, copy.right);
    }

    copy.dataType = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN);
    copy.dataType.nullable = std::ranges::any_of(copy.getChildren(), [](const auto& child) { return child.getDataType().nullable; });
    return copy;
}

Reflected Reflector<AndLogicalFunction>::operator()(const AndLogicalFunction& function) const
{
    return reflect(detail::ReflectedAndLogicalFunction{.left = function.left, .right = function.right});
}

AndLogicalFunction Unreflector<AndLogicalFunction>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [left, right] = context.unreflect<detail::ReflectedAndLogicalFunction>(rfl);
    return {std::move(left), std::move(right)};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAndLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("AndLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return AndLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
