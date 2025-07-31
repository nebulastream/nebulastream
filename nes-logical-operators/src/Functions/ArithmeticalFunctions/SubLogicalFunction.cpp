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

#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>

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

SubLogicalFunction::SubLogicalFunction(LogicalFunction left, LogicalFunction right) : left(std::move(left)), right(std::move(right)) { };

bool SubLogicalFunction::operator==(const SubLogicalFunction& rhs) const
{
    return left == rhs.left and right == rhs.right;
}

std::string SubLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SubLogicalFunction({} - {} : {})", left.explain(verbosity), right.explain(verbosity), dataType);
    }
    return fmt::format("{} - {}", left.explain(verbosity), right.explain(verbosity));
}

DataType SubLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction SubLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.left = left.withInferredDataType(schema);
    copy.right = right.withInferredDataType(schema);
    auto newDataType = copy.left.getDataType().join(copy.right.getDataType());
    if (not newDataType.has_value())
    {
        throw CannotInferStamp("Cannot apply subtraction to input function left: {}, right: {}", copy.left, copy.right);
    }
    copy.dataType = std::move(newDataType).value();
    copy.dataType.nullable = std::ranges::any_of(copy.getChildren(), [](const auto& child) { return child.getDataType().nullable; });
    return copy;
};

std::vector<LogicalFunction> SubLogicalFunction::getChildren() const
{
    return {left, right};
};

SubLogicalFunction SubLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "SubLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.dataType
        = children[0].getDataType().join(children[1].getDataType()).value_or(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED));
    return copy;
};

std::string_view SubLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<SubLogicalFunction>::operator()(const SubLogicalFunction& function) const
{
    return reflect(detail::ReflectedSubLogicalFunction{.left = function.left, .right = function.right});
}

SubLogicalFunction Unreflector<SubLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [left, right] = context.unreflect<detail::ReflectedSubLogicalFunction>(reflected);
    return SubLogicalFunction{std::move(left), std::move(right)};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterSubLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("Function requires exactly two children, but got {}", arguments.children.size());
    }
    return SubLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
