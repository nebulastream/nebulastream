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

#include <Functions/SemanticAggAggregationLogicalFunction.hpp>

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <magic_enum/magic_enum.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

SemanticAggAggregationLogicalFunction::SemanticAggAggregationLogicalFunction(std::string modelName, AggregationFieldAccess inputFunction)
    : modelName(std::move(modelName)), inputFunction(std::move(inputFunction))
{
}

std::string_view SemanticAggAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

DataType SemanticAggAggregationLogicalFunction::getAggregateType()
{
    return DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE);
}

bool SemanticAggAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

AggregationFieldAccess SemanticAggAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string SemanticAggAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("SEM_AGG({}, {})", modelName, inputExplain);
}

bool SemanticAggAggregationLogicalFunction::operator==(const SemanticAggAggregationLogicalFunction& other) const
{
    return modelName == other.modelName && inputFunction == other.inputFunction;
}

SemanticAggAggregationLogicalFunction SemanticAggAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    auto newInputFunction = inferFieldAccess(inputFunction, schema);
    /// It becomes JSON_AGG, which takes NOT NULL strings.
    if (!newInputFunction->getDataType().isType(DataType::Type::VARSIZED) || newInputFunction->getDataType().nullable)
    {
        throw UnsupportedQuery(
            "SEM_AGG is only supported on NOT NULL VARSIZED fields, but field {} has type {}{}",
            newInputFunction->getField().getLastName(),
            magic_enum::enum_name(newInputFunction->getDataType().type),
            newInputFunction->getDataType().nullable ? " (nullable)" : "");
    }
    return SemanticAggAggregationLogicalFunction{modelName, newInputFunction};
}

Reflected Reflector<SemanticAggAggregationLogicalFunction>::operator()(
    const SemanticAggAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedSemanticAggAggregationLogicalFunction{
            .modelName = function.getModelName(), .inputFunction = function.getInputFunction()});
}

SemanticAggAggregationLogicalFunction
Unreflector<SemanticAggAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [modelName, inputFunction] = context.unreflect<detail::ReflectedSemanticAggAggregationLogicalFunction>(reflected);
    return SemanticAggAggregationLogicalFunction{std::move(modelName), std::move(inputFunction)};
}

AggregationLogicalFunctionRegistryReturnType
SemanticAggAggregationLogicalFunction::create(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 2)
    {
        throw InvalidQuerySyntax("SEM_AGG expects a semantic model name and one field, but got {} arguments", arguments.on.size());
    }
    const auto* const model = std::get_if<TypedLogicalFunction<UnboundFieldAccessLogicalFunction>>(&arguments.on.at(0));
    if (model == nullptr)
    {
        throw InvalidQuerySyntax("SEM_AGG expects a semantic model name as its first argument");
    }
    return SemanticAggAggregationLogicalFunction{(*model)->getFieldName().asCanonicalString(), arguments.on.at(1)};
}

}

size_t
std::hash<NES::SemanticAggAggregationLogicalFunction>::operator()(const NES::SemanticAggAggregationLogicalFunction& function) const noexcept
{
    return folly::hash::hash_combine(
        function.getModelName(), function.getInputFunction(), NES::SemanticAggAggregationLogicalFunction::getName());
}
