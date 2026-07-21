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
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Collects fixed-size field values into an insertion-ordered raw byte payload.
/// The VARSIZED result intentionally carries no element metadata yet; callers must know the input type.
class ArrayAggUnsortedAggregationLogicalFunction
{
public:
    explicit ArrayAggUnsortedAggregationLogicalFunction(AggregationFieldAccess inputFunction);

    [[nodiscard]] ArrayAggUnsortedAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const ArrayAggUnsortedAggregationLogicalFunction& other) const;

private:
    AggregationFieldAccess inputFunction;
    static constexpr std::string_view NAME = "ArrayAggUnsorted";
};

template <>
struct Reflector<ArrayAggUnsortedAggregationLogicalFunction>
{
    Reflected operator()(const ArrayAggUnsortedAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<ArrayAggUnsortedAggregationLogicalFunction>
{
    ArrayAggUnsortedAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::ArrayAggUnsortedAggregationLogicalFunction>
{
    size_t operator()(const NES::ArrayAggUnsortedAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::ArrayAggUnsortedAggregationLogicalFunction>);
