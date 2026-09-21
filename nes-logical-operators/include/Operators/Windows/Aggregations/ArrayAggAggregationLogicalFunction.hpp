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
#include <functional>
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
/// Collects fixed-size values by preserving order within input buffers and sorting whole buffer pages by their first timestamp.
class ArrayAggAggregationLogicalFunction
{
public:
    explicit ArrayAggAggregationLogicalFunction(AggregationFieldAccess inputFunction);
    [[nodiscard]] ArrayAggAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const ArrayAggAggregationLogicalFunction& other) const;

    static AggregationLogicalFunctionRegistryReturnType create(AggregationLogicalFunctionRegistryArguments arguments);

private:
    AggregationFieldAccess inputFunction;
    static constexpr std::string_view NAME = "ArrayAgg";
};

template <>
struct Reflector<ArrayAggAggregationLogicalFunction>
{
    Reflected operator()(const ArrayAggAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<ArrayAggAggregationLogicalFunction>
{
    ArrayAggAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::ArrayAggAggregationLogicalFunction>
{
    size_t operator()(const NES::ArrayAggAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::ArrayAggAggregationLogicalFunction>);
