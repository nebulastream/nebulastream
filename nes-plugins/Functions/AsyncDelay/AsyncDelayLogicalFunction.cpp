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

#include <Functions/AsyncDelayLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

AsyncDelayLogicalFunction::AsyncDelayLogicalFunction(LogicalFunction value, LogicalFunction millis)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), value(std::move(value)), millis(std::move(millis))
{
}

bool AsyncDelayLogicalFunction::operator==(const AsyncDelayLogicalFunction& rhs) const
{
    return value == rhs.value and millis == rhs.millis;
}

DataType AsyncDelayLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction AsyncDelayLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.value = value.withInferredDataType(schema);
    copy.millis = millis.withInferredDataType(schema);
    if (not copy.value.getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("ASYNC_DELAY expects a VARSIZED value, but got {}", copy.value);
    }
    /// The Async source reads only VARSIZED fields, so the delay has to be a constant.
    if (not copy.millis.tryGetAs<ConstantValueLogicalFunction>().has_value() or not copy.millis.getDataType().isInteger())
    {
        throw DifferentFieldTypeExpected("ASYNC_DELAY expects an integer constant delay in milliseconds, but got {}", copy.millis);
    }
    return copy;
}

std::vector<LogicalFunction> AsyncDelayLogicalFunction::getChildren() const
{
    return {value, millis};
}

AsyncDelayLogicalFunction AsyncDelayLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.value = children.at(0);
    copy.millis = children.at(1);
    return copy;
}

std::string_view AsyncDelayLogicalFunction::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

std::string AsyncDelayLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("ASYNC_DELAY({}, {})", value.explain(verbosity), millis.explain(verbosity));
}

Reflected Reflector<AsyncDelayLogicalFunction>::operator()(const AsyncDelayLogicalFunction& function, const ReflectionContext& context) const
{
    const auto children = function.getChildren();
    return context.reflect(detail::ReflectedAsyncDelayLogicalFunction{.value = children.at(0), .millis = children.at(1)});
}

AsyncDelayLogicalFunction Unreflector<AsyncDelayLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [value, millis] = context.unreflect<detail::ReflectedAsyncDelayLogicalFunction>(reflected);
    return AsyncDelayLogicalFunction{std::move(value), std::move(millis)};
}

LogicalFunctionRegistryReturnType AsyncDelayLogicalFunction::createASYNC_DELAY(LogicalFunctionRegistryArguments arguments)
{
    /// The argument count comes straight from the user's query, so a bad count is reported rather than asserted.
    if (arguments.children.size() != 2)
    {
        throw InvalidQuerySyntax("ASYNC_DELAY expects 2 arguments (value, millis), but got {}", arguments.children.size());
    }
    return AsyncDelayLogicalFunction{arguments.children.at(0), arguments.children.at(1)};
}

}
