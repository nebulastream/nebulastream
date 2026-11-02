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

#include <Functions/SemanticCallLogicalFunction.hpp>

#include <cstddef>
#include <cstdint>
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
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SemanticCallPayload.hpp>

namespace NES
{

namespace
{
/// FNV-1a: stable across processes and platforms, so an EXPLAIN output can be compared against a file.
uint32_t payloadHash(const std::string_view payload)
{
    uint64_t hash = 14695981039346656037ULL;
    for (const char character : payload)
    {
        hash ^= static_cast<unsigned char>(character);
        hash *= 1099511628211ULL;
    }
    return static_cast<uint32_t>(hash ^ (hash >> 32U));
}
}

SemanticCallLogicalFunction::SemanticCallLogicalFunction(std::string payload, std::vector<LogicalFunction> inputs)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), payload(std::move(payload)), inputs(std::move(inputs))
{
    const auto decoded = decodeSemanticCallPayload(this->payload);
    model = decoded.model;
    /// A SEM_SCAN or SEM_WINDOW call has one value per OUTPUT field; the other calls one per step.
    numberOfSteps = decoded.config.keyed ? decoded.outputFields.size() : decoded.config.steps.size();
    keyed = decoded.config.keyed.has_value();
    if (numberOfSteps == 0)
    {
        throw InvalidSemanticModel("SEM_CALL of model '{}' has no steps", model);
    }
}

bool SemanticCallLogicalFunction::operator==(const SemanticCallLogicalFunction& rhs) const
{
    return payload == rhs.payload and inputs == rhs.inputs;
}

DataType SemanticCallLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction SemanticCallLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    for (auto& input : copy.inputs)
    {
        input = input.withInferredDataType(schema);
        /// The Async source hands only VARSIZED fields to a call.
        if (not input.getDataType().isType(DataType::Type::VARSIZED))
        {
            throw DifferentFieldTypeExpected("SEM_CALL of model '{}' expects VARSIZED inputs, but got {}", model, input);
        }
    }
    return copy;
}

std::vector<LogicalFunction> SemanticCallLogicalFunction::getChildren() const
{
    return inputs;
}

SemanticCallLogicalFunction SemanticCallLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.inputs = children;
    return copy;
}

std::string_view SemanticCallLogicalFunction::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

std::string SemanticCallLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::vector<std::string> arguments{fmt::format("{}:{:08x}", model, payloadHash(payload))};
    for (const auto& input : inputs)
    {
        arguments.push_back(input.explain(verbosity));
    }
    return fmt::format("SEM_CALL({})", fmt::join(arguments, ", "));
}

Reflected
Reflector<SemanticCallLogicalFunction>::operator()(const SemanticCallLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedSemanticCallLogicalFunction{.payload = function.getPayload(), .inputs = function.getChildren()});
}

SemanticCallLogicalFunction
Unreflector<SemanticCallLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [payload, inputs] = context.unreflect<detail::ReflectedSemanticCallLogicalFunction>(reflected);
    return SemanticCallLogicalFunction{std::move(payload), std::move(inputs)};
}

LogicalFunctionRegistryReturnType SemanticCallLogicalFunction::createSEM_CALL(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.empty())
    {
        throw InvalidQuerySyntax("SEM_CALL expects a constant payload followed by its inputs");
    }
    const auto payload = arguments.children.front().tryGetAs<ConstantValueLogicalFunction>();
    if (not payload.has_value())
    {
        throw InvalidQuerySyntax("SEM_CALL expects a constant payload as its first argument, but got {}", arguments.children.front());
    }
    std::vector inputs(arguments.children.begin() + 1, arguments.children.end());
    return SemanticCallLogicalFunction{payload.value()->getConstantValue(), std::move(inputs)};
}

}
