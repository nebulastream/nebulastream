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

// Generated from: square.extension.md

#include <SquareLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
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

SquareLogicalFunction::SquareLogicalFunction(LogicalFunction child) : child(std::move(child))
{
}

DataType SquareLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction SquareLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    SquareLogicalFunction copy = *this;
    copy.child = child.withInferredDataType(schema);
    if (!copy.child.getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot apply square function on non-numeric input function {}", copy.child);
    }
    copy.dataType = copy.child.getDataType();
    return copy;
};

std::vector<LogicalFunction> SquareLogicalFunction::getChildren() const
{
    return {child};
};

SquareLogicalFunction SquareLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "SquareLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view SquareLogicalFunction::getType() const
{
    return NAME;
}

bool SquareLogicalFunction::operator==(const SquareLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string SquareLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SquareLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("SQUARE({})", child.explain(verbosity));
}

Reflected Reflector<SquareLogicalFunction>::operator()(const SquareLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedSquareLogicalFunction{.child = function.child});
}

SquareLogicalFunction Unreflector<SquareLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [child] = context.unreflect<detail::ReflectedSquareLogicalFunction>(reflected);
    return SquareLogicalFunction(std::move(child));
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterSquareLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("SquareLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return SquareLogicalFunction(arguments.children[0]);
}

}
