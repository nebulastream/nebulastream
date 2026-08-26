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
