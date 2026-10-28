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

#include <Functions/AsyncOutputLogicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/AsyncLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// Values are VARSIZED, like every argument and result the Async source handles
AsyncOutputLogicalFunction::AsyncOutputLogicalFunction(LogicalFunction call, const size_t index)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), call(std::move(call)), index(index)
{
}

bool AsyncOutputLogicalFunction::operator==(const AsyncOutputLogicalFunction& rhs) const
{
    return call == rhs.call and index == rhs.index;
}

DataType AsyncOutputLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction AsyncOutputLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.call = call.withInferredDataType(schema);
    const auto async = copy.call.tryGetAs<AsyncLogicalFunction>();
    if (not async.has_value() or copy.call.tryGetAs<AsyncOutputLogicalFunction>().has_value())
    {
        throw InvalidQuerySyntax("ASYNC_OUTPUT reads a value of an async function call, but got {}", copy.call);
    }
    if (index >= async.value()->get().getNumberOfAsyncOutputs())
    {
        throw InvalidQuerySyntax(
            "ASYNC_OUTPUT reads value {} of {}, which produces {}", index, copy.call, async.value()->get().getNumberOfAsyncOutputs());
    }
    return copy;
}

std::vector<LogicalFunction> AsyncOutputLogicalFunction::getChildren() const
{
    return {call};
}

AsyncOutputLogicalFunction AsyncOutputLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.call = children.at(0);
    return copy;
}

std::string_view AsyncOutputLogicalFunction::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

std::string AsyncOutputLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("ASYNC_OUTPUT({}, {})", call.explain(verbosity), index);
}

Reflected
Reflector<AsyncOutputLogicalFunction>::operator()(const AsyncOutputLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedAsyncOutputLogicalFunction{.call = function.getCall(), .index = function.getIndex()});
}

AsyncOutputLogicalFunction
Unreflector<AsyncOutputLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [call, index] = context.unreflect<detail::ReflectedAsyncOutputLogicalFunction>(reflected);
    return AsyncOutputLogicalFunction{std::move(call), index};
}

LogicalFunctionRegistryReturnType AsyncOutputLogicalFunction::createASYNC_OUTPUT(LogicalFunctionRegistryArguments arguments)
{
    /// The arguments come straight from the user's query, so bad ones are reported rather than asserted.
    if (arguments.children.size() != 2)
    {
        throw InvalidQuerySyntax("ASYNC_OUTPUT expects 2 arguments (call, index), but got {}", arguments.children.size());
    }
    const auto constant = arguments.children.at(1).tryGetAs<ConstantValueLogicalFunction>();
    const auto index = constant.has_value() ? from_chars<uint64_t>(constant.value()->getConstantValue()) : std::nullopt;
    if (not index.has_value())
    {
        throw InvalidQuerySyntax("ASYNC_OUTPUT expects a non-negative integer constant as index, but got {}", arguments.children.at(1));
    }
    return AsyncOutputLogicalFunction{arguments.children.at(0), *index};
}

}
