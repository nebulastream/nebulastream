/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
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

class LastAggregationLogicalFunction
{
public:
    explicit LastAggregationLogicalFunction(AggregationFieldAccess inputFunction);
    LastAggregationLogicalFunction(AggregationFieldAccess inputFunction, DataType aggregateType);

    [[nodiscard]] LastAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] bool operator==(const LastAggregationLogicalFunction& other) const;

private:
    AggregationFieldAccess inputFunction;
    DataType aggregateType;
    static constexpr std::string_view NAME = "Last";
};

template <>
struct Reflector<LastAggregationLogicalFunction>
{
    Reflected operator()(const LastAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<LastAggregationLogicalFunction>
{
    LastAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::LastAggregationLogicalFunction>
{
    size_t operator()(const NES::LastAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::LastAggregationLogicalFunction>);
