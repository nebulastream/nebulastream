/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0
*/

#include <Aggregation/Function/ArrayAggAggregationPhysicalFunction.hpp>

#include <memory>
#include <utility>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterArrayAggUnsortedAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "ARRAY_AGG_UNSORTED tuple layout not set");
    return std::make_shared<ArrayAggAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        std::move(arguments.inputFunction),
        std::move(arguments.resultFieldIdentifier),
        std::move(arguments.tupleLayout.value()),
        false);
}
}
