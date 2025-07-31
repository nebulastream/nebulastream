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

#include <Operators/Windows/Aggregations/MinAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <Schema/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
MinAggregationLogicalFunction::MinAggregationLogicalFunction(LogicalFunction inputFunction)
    : WindowAggregationLogicalFunction(std::move(inputFunction))
{
}

std::string_view MinAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

std::shared_ptr<WindowAggregationLogicalFunction> MinAggregationLogicalFunction::withInferredType(const Schema& schema)
{
    auto newInputFunction = inputFunction.withInferredDataType(schema);
    if (!newInputFunction.getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot calculate minimum value on non-numeric function {}.", newInputFunction);
    }
    return std::make_shared<MinAggregationLogicalFunction>(newInputFunction);
}

SerializableAggregationFunction MinAggregationLogicalFunction::serialize() const
{
    SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto inputFunc = SerializableFunction();
    inputFunc.CopyFrom(inputFunction.serialize());

    serializedAggregationFunction.mutable_on_fields()->Add()->CopyFrom(inputFunc);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterMinAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("MinAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return std::make_shared<MinAggregationLogicalFunction>(arguments.on.at(0));
}
}
