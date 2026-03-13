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

#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

DivLogicalFunction::DivLogicalFunction(LogicalFunction left, LogicalFunction right) : left(std::move(left)), right(std::move(right)) { };

bool DivLogicalFunction::operator==(const DivLogicalFunction& rhs) const
{
    return left == rhs.left and right == rhs.right;
}

DataType DivLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction DivLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.left = left.withInferredDataType(schema);
    copy.right = right.withInferredDataType(schema);
    if (!copy.left.getDataType().isNumeric() || !copy.right.getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot apply division to non-numeric input function left: {}, right: {}", copy.left, copy.right);
    }
    auto newDataType = copy.left.getDataType().join(copy.right.getDataType());
    if (!newDataType.has_value())
    {
        throw CannotInferStamp("Could not join datatypes {} and {} for division", copy.left.getDataType(), copy.right.getDataType());
    }
    copy.dataType = std::move(newDataType).value();
    copy.dataType.nullable = std::ranges::any_of(copy.getChildren(), [](const auto& child) { return child.getDataType().nullable; });

    return copy;
};

std::vector<LogicalFunction> DivLogicalFunction::getChildren() const
{
    return {left, right};
};

DivLogicalFunction DivLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "DivLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.dataType
        = children[0].getDataType().join(children[1].getDataType()).value_or(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED));
    return copy;
};

std::string_view DivLogicalFunction::getType() const
{
    return NAME;
}

std::string DivLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("DivLogicalFunction({} / {} : {})", left.explain(verbosity), right.explain(verbosity), dataType);
    }
    return fmt::format("{} / {}", left.explain(verbosity), right.explain(verbosity));
}

Reflected Reflector<DivLogicalFunction>::operator()(const DivLogicalFunction& function) const
{
    return reflect(detail::ReflectedDivLogicalFunction{.left = function.left, .right = function.right});
}

DivLogicalFunction Unreflector<DivLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [left, right] = context.unreflect<detail::ReflectedDivLogicalFunction>(reflected);
    return DivLogicalFunction{std::move(left), std::move(right)};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterDivLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("Function requires exactly two children, but got {}", arguments.children.size());
    }
    return DivLogicalFunction(arguments.children[0], arguments.children[1]);
}


}
