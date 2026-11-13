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

#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/CheckedInteger.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/CheckedDeadline.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

AsyncDelayLogicalFunction::AsyncDelayLogicalFunction(
    LogicalFunction value, LogicalFunction millis, std::optional<LogicalFunction> maxConcurrency)
    : dataType(value.getDataType()), value(std::move(value)), millis(std::move(millis)), maxConcurrency(std::move(maxConcurrency))
{
}

bool AsyncDelayLogicalFunction::operator==(const AsyncDelayLogicalFunction& rhs) const
{
    return value == rhs.value and millis == rhs.millis and maxConcurrency == rhs.maxConcurrency;
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
    copy.dataType = copy.value.getDataType();
    if (not copy.millis.getDataType().isInteger() or copy.millis.getDataType().nullable)
    {
        throw DifferentFieldTypeExpected("ASYNC_DELAY expects a non-nullable integer delay in milliseconds, but got {}", copy.millis);
    }
    if (const auto constant = copy.millis.tryGetAs<ConstantValueLogicalFunction>())
    {
        const auto value = RuntimeValue::fromLiteral(constant->getDataType(), constant->get().getConstantValue());
        try
        {
            const auto millis = checkedUnsignedIntegerOption(value.view(), "ASYNC_DELAY millis");
            static_cast<void>(checkedSteadyDeadline(millis, "ASYNC_DELAY"));
        }
        catch (const Exception&)
        {
            throw CannotInferSchema("ASYNC_DELAY constant delay must be a representable non-negative integer");
        }
    }
    if (maxConcurrency)
    {
        copy.maxConcurrency = maxConcurrency->withInferredDataType(schema);
        if (not copy.maxConcurrency->getDataType().isInteger() or copy.maxConcurrency->getDataType().nullable)
        {
            throw DifferentFieldTypeExpected(
                "ASYNC_DELAY expects a non-nullable integer concurrency limit, but got {}", *copy.maxConcurrency);
        }
        const auto constant = copy.maxConcurrency->tryGetAs<ConstantValueLogicalFunction>();
        if (not constant.has_value())
        {
            throw CannotInferSchema("ASYNC_DELAY requires a constant concurrency limit, but got {}", *copy.maxConcurrency);
        }
        const auto value = RuntimeValue::fromLiteral(constant->getDataType(), constant->get().getConstantValue());
        try
        {
            const auto limit = checkedUnsignedIntegerOption(value.view(), "ASYNC_DELAY concurrency");
            if (limit == 0 || !std::in_range<size_t>(limit))
            {
                throw ArithmeticalError("ASYNC_DELAY concurrency must be positive and representable");
            }
        }
        catch (const Exception&)
        {
            throw CannotInferSchema("ASYNC_DELAY constant concurrency must be positive and representable");
        }
    }
    return copy;
}

std::vector<LogicalFunction> AsyncDelayLogicalFunction::getChildren() const
{
    if (maxConcurrency)
    {
        return {value, millis, *maxConcurrency};
    }
    return {value, millis};
}

AsyncDelayLogicalFunction AsyncDelayLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2 || children.size() == 3, "ASYNC_DELAY takes 2 or 3 children");
    auto copy = *this;
    copy.value = children.at(0);
    copy.dataType = copy.value.getDataType();
    copy.millis = children.at(1);
    copy.maxConcurrency = children.size() > 2 ? std::optional{children.at(2)} : std::nullopt;
    return copy;
}

std::string_view AsyncDelayLogicalFunction::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

std::string AsyncDelayLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (maxConcurrency)
    {
        return fmt::format(
            "ASYNC_DELAY({}, {}, {})", value.explain(verbosity), millis.explain(verbosity), maxConcurrency->explain(verbosity));
    }
    return fmt::format("ASYNC_DELAY({}, {})", value.explain(verbosity), millis.explain(verbosity));
}

std::vector<DataType> AsyncDelayLogicalFunction::getAsyncOutputTypes() const
{
    return {dataType, DataTypeProvider::provideDataType(DataType::Type::UINT64)};
}

Reflected
Reflector<AsyncDelayLogicalFunction>::operator()(const AsyncDelayLogicalFunction& function, const ReflectionContext& context) const
{
    const auto children = function.getChildren();
    return context.reflect(
        detail::ReflectedAsyncDelayLogicalFunction{
            .value = children.at(0),
            .millis = children.at(1),
            .maxConcurrency = children.size() > 2 ? std::optional{children.at(2)} : std::nullopt});
}

AsyncDelayLogicalFunction
Unreflector<AsyncDelayLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [value, millis, maxConcurrency] = context.unreflect<detail::ReflectedAsyncDelayLogicalFunction>(reflected);
    return AsyncDelayLogicalFunction{std::move(value), std::move(millis), std::move(maxConcurrency)};
}

LogicalFunctionRegistryReturnType AsyncDelayLogicalFunction::createASYNC_DELAY(LogicalFunctionRegistryArguments arguments)
{
    /// The argument count comes straight from the user's query, so a bad count is reported rather than asserted.
    if (arguments.children.size() != 2 and arguments.children.size() != 3)
    {
        throw InvalidQuerySyntax(
            "ASYNC_DELAY expects 2 or 3 arguments (value, millis[, max_concurrency]), but got {}", arguments.children.size());
    }
    return AsyncDelayLogicalFunction{
        arguments.children.at(0),
        arguments.children.at(1),
        arguments.children.size() > 2 ? std::optional{arguments.children.at(2)} : std::nullopt};
}

}
