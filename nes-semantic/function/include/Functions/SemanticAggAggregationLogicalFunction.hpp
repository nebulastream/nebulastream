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

#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include <DataTypes/DataType.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>

namespace NES
{

/// `SEM_AGG(model, field) AS name` in a windowed aggregation: one answer of a semantic map model for all values of
/// `field` in the window (per group). A placeholder that is never executed: `SemanticCallTranslationRule` replaces it
/// with `JSON_AGG(field)` and a `SEM_CALL` on that array above the window. VARSIZED, NOT NULL.
///
/// The parser hands the model name over as the first field access, as for SEM_MATCH.
class SemanticAggAggregationLogicalFunction
{
public:
    SemanticAggAggregationLogicalFunction(std::string modelName, AggregationFieldAccess inputFunction);

    [[nodiscard]] SemanticAggAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] static DataType getAggregateType();
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;

    [[nodiscard]] const std::string& getModelName() const { return modelName; }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const SemanticAggAggregationLogicalFunction& other) const;

    static AggregationLogicalFunctionRegistryReturnType create(AggregationLogicalFunctionRegistryArguments arguments);

private:
    std::string modelName;
    AggregationFieldAccess inputFunction;
    static constexpr std::string_view NAME = "SemanticAgg";
};

namespace detail
{
struct ReflectedSemanticAggAggregationLogicalFunction
{
    std::string modelName;
    AggregationFieldAccess inputFunction;
};
}

template <>
struct Reflector<SemanticAggAggregationLogicalFunction>
{
    Reflected operator()(const SemanticAggAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<SemanticAggAggregationLogicalFunction>
{
    SemanticAggAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::SemanticAggAggregationLogicalFunction>
{
    size_t operator()(const NES::SemanticAggAggregationLogicalFunction& function) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::SemanticAggAggregationLogicalFunction>);
