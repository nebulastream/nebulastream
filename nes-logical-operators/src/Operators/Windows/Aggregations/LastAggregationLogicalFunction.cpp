/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
*/

#include <Operators/Windows/Aggregations/LastAggregationLogicalFunction.hpp>

#include <memory>
#include <utility>

#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <folly/hash/Hash.h>
#include <fmt/format.h>

namespace NES
{
LastAggregationLogicalFunction::LastAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(inputFunction), aggregateType(std::visit([](const auto& input) { return input->getDataType(); }, inputFunction))
{
}

LastAggregationLogicalFunction::LastAggregationLogicalFunction(AggregationFieldAccess inputFunction, DataType aggregateType)
    : inputFunction(std::move(inputFunction)), aggregateType(aggregateType)
{
}

bool LastAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

std::string_view LastAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

DataType LastAggregationLogicalFunction::getAggregateType() const
{
    return aggregateType;
}

AggregationFieldAccess LastAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string LastAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    return fmt::format("{}({})", NAME, std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction));
}

bool LastAggregationLogicalFunction::operator==(const LastAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction && aggregateType == other.aggregateType;
}

LastAggregationLogicalFunction LastAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    auto newInputFunction = inferFieldAccess(inputFunction, schema);
    return LastAggregationLogicalFunction{newInputFunction, newInputFunction->getDataType()};
}

Reflected Reflector<LastAggregationLogicalFunction>::operator()(
    const LastAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(function.getInputFunction());
}

LastAggregationLogicalFunction
Unreflector<LastAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return LastAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterLastAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("LastAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return LastAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t std::hash<NES::LastAggregationLogicalFunction>::operator()(
    const NES::LastAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), NES::LastAggregationLogicalFunction::getName());
}
