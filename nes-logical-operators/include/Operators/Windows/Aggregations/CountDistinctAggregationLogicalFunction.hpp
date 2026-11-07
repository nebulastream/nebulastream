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
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class CountDistinctAggregationLogicalFunction
{
public:
    explicit CountDistinctAggregationLogicalFunction(AggregationFieldAccess inputFunction);

    [[nodiscard]] CountDistinctAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] static DataType getAggregateType();
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const CountDistinctAggregationLogicalFunction& other) const;

    static AggregationLogicalFunctionRegistryReturnType create(AggregationLogicalFunctionRegistryArguments arguments);

private:
    AggregationFieldAccess inputFunction;
    static constexpr std::string_view NAME = "CountDistinct";
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::UINT64;
};

template <>
struct Reflector<CountDistinctAggregationLogicalFunction>
{
    Reflected operator()(const CountDistinctAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<CountDistinctAggregationLogicalFunction>
{
    CountDistinctAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::CountDistinctAggregationLogicalFunction>
{
    size_t operator()(const NES::CountDistinctAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::CountDistinctAggregationLogicalFunction>);
