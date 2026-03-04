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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>

#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class CountAggregationLogicalFunction
{
public:
    explicit CountAggregationLogicalFunction(AggregationFieldAccess inputFunction, bool includeNullValues);

    [[nodiscard]] CountAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] static DataType getAggregateType();
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] bool shallIncludeNullValues() const noexcept;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const CountAggregationLogicalFunction& other) const;

private:
    AggregationFieldAccess inputFunction;
    bool includeNullValues = false;
    static constexpr std::string_view NAME = "Count";
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::UINT64;
};

template <>
struct Reflector<CountAggregationLogicalFunction>
{
    Reflected operator()(const CountAggregationLogicalFunction& function) const;
};

template <>
struct Unreflector<CountAggregationLogicalFunction>
{
    CountAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}

template <>
struct std::hash<NES::CountAggregationLogicalFunction>
{
    size_t operator()(const NES::CountAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::CountAggregationLogicalFunction>);
