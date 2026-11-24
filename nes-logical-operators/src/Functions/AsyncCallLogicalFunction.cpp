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

#include <Functions/AsyncCallLogicalFunction.hpp>

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncOperatorReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <folly/hash/Hash.h>
#include <AsyncOperatorRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
AsyncOperator createAsyncConfig(const std::string& name, const std::vector<LogicalFunction>& arguments)
{
    std::vector<DataType> types;
    types.reserve(arguments.size());
    for (const auto& argument : arguments)
    {
        types.push_back(argument.getDataType());
    }
    return createAsyncOperator(name, std::move(types));
}
}

AsyncCallLogicalFunction::AsyncCallLogicalFunction(const std::string& asyncOperatorName, std::vector<LogicalFunction> args)
    : implementation(createAsyncConfig(asyncOperatorName, args)), arguments(std::move(args))
{
    validateScalar();
}

AsyncCallLogicalFunction::AsyncCallLogicalFunction(AsyncOperator implementation, std::vector<LogicalFunction> arguments)
    : implementation(std::move(implementation)), arguments(std::move(arguments))
{
}

AsyncCallLogicalFunction AsyncCallLogicalFunction::fromOperator(AsyncOperator implementation, std::vector<LogicalFunction> arguments)
{
    return AsyncCallLogicalFunction{std::move(implementation), std::move(arguments)};
}

bool AsyncCallLogicalFunction::operator==(const AsyncCallLogicalFunction& rhs) const
{
    return getOperatorName() == rhs.getOperatorName() && implementation.getOutputSchema() == rhs.implementation.getOutputSchema()
        && implementation.maxConcurrency() == rhs.implementation.maxConcurrency()
        && std::ranges::equal(arguments, rhs.arguments, StructuralFunctionEqual{});
}

void AsyncCallLogicalFunction::validateScalar() const
{
    if (implementation.getOutputSchema().size() != 1)
    {
        throw InvalidQuerySyntax(
            "Async operator '{}' cannot be used as a scalar expression: expected exactly one output", getOperatorName());
    }
}

AsyncCallLogicalFunction AsyncCallLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.arguments = children;
    if (copy.getOutputName() != getOutputName() || copy.getOperatorName() != getOperatorName())
    {
        throw InvalidQuerySyntax("Async scalar rewriting changed its public name");
    }
    return copy;
}

LogicalFunction AsyncCallLogicalFunction::withInferredDataType(TypeInferenceContext& context) const
{
    auto copy = *this;
    copy.arguments.clear();
    copy.arguments.reserve(arguments.size());
    for (const auto& argument : arguments)
    {
        copy.arguments.push_back(context.inferType(argument));
    }
    copy.validateScalar();
    if (copy.getOutputName() != getOutputName() || copy.getOperatorName() != getOperatorName())
    {
        throw CannotInferSchema("Async scalar inference changed its public name");
    }
    return copy;
}

std::string AsyncCallLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    const auto arguments = getChildren();
    std::string result = getOperatorName() + "(";
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        if (i != 0U)
        {
            result += ", ";
        }
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) i is bounded by arguments.size()
        result += arguments[i].explain(verbosity);
    }
    return result + ")";
}

Reflected Reflector<AsyncCallLogicalFunction>::operator()(const AsyncCallLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedAsyncCallLogicalFunction{
            .implementation = function.getAsyncOperator(),
            .arguments = function.getArguments(),
            .outputName = function.getOutputName(),
            .dataType = function.getDataType()});
}

AsyncCallLogicalFunction
Unreflector<AsyncCallLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto value = context.unreflect<detail::ReflectedAsyncCallLogicalFunction>(reflected);
    AsyncCallLogicalFunction result(std::move(value.implementation), std::move(value.arguments));
    result.validateScalar();
    if (result.getOutputName() != value.outputName || result.getDataType() != value.dataType)
    {
        throw CannotDeserialize("AsyncCall payload does not match the serialized signature");
    }
    return result;
}
}

size_t std::hash<NES::AsyncCallLogicalFunction>::operator()(const NES::AsyncCallLogicalFunction& function) const noexcept
{
    size_t seed = folly::hash::hash_combine(function.getOperatorName(), function.getOutputName(), function.getDataType());
    for (const auto& arg : function.getArguments())
    {
        seed = folly::hash::hash_combine(seed, NES::StructuralFunctionHash{}(arg));
    }
    return seed;
}
