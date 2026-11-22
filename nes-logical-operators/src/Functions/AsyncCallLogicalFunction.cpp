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

#include <cstddef>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/AsyncArgumentsWithConstants.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncOperatorReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/UUID.hpp>
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

AsyncCallLogicalFunction::AsyncCallLogicalFunction(const std::string& name, std::vector<LogicalFunction> args)
    : implementation(createAsyncConfig(name, args)), arguments(std::move(args)), occurrence(AsyncInvocationId{generateUUID()})
{
    validateScalar();
}

AsyncCallLogicalFunction::AsyncCallLogicalFunction(
    AsyncOperator implementation, std::vector<LogicalFunction> arguments, AsyncInvocationId occurrence)
    : implementation(std::move(implementation)), arguments(std::move(arguments)), occurrence(occurrence)
{
    validateScalar();
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
    copy.validateScalar();
    if (copy.getOutputName() != getOutputName() || copy.getOperatorName() != getOperatorName())
    {
        throw InvalidQuerySyntax("Async scalar rewriting changed its public name");
    }
    return copy;
}

LogicalFunction AsyncCallLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.arguments.clear();
    copy.arguments.reserve(arguments.size());
    for (const auto& argument : arguments)
    {
        copy.arguments.push_back(argument.withInferredDataType(schema));
    }
    copy.implementation = implementation.withInferredArguments(makeAsyncArgumentsWithConstants(copy.arguments));
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
            .dataType = function.getDataType(),
            .occurrence = function.getOccurrenceId()});
}

AsyncCallLogicalFunction
Unreflector<AsyncCallLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto value = context.unreflect<detail::ReflectedAsyncCallLogicalFunction>(reflected);
    AsyncCallLogicalFunction result(std::move(value.implementation), std::move(value.arguments), value.occurrence);
    if (result.getOutputName() != value.outputName || result.getDataType() != value.dataType)
    {
        throw CannotDeserialize("AsyncCall payload does not match the serialized signature");
    }
    return result;
}
}
