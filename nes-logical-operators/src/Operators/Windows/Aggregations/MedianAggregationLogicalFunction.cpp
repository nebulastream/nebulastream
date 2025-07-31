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

#include <Operators/Windows/Aggregations/MedianAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

#include <utility>
#include <AggregationLogicalFunctionRegistry.hpp>

namespace NES
{
MedianAggregationLogicalFunction::MedianAggregationLogicalFunction(LogicalFunction inputFunction)
    : WindowAggregationLogicalFunction(std::move(inputFunction))
{
}

std::string_view MedianAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

std::shared_ptr<WindowAggregationLogicalFunction> MedianAggregationLogicalFunction::withInferredType(const Schema& schema)
{
    auto newInputFunction = inputFunction.withInferredDataType(schema);
    if (!newInputFunction.getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot calculate median over non numeric fields.", newInputFunction.getDataType().isNumeric());
    }
    return std::make_shared<MedianAggregationLogicalFunction>(newInputFunction);
}

SerializableAggregationFunction MedianAggregationLogicalFunction::serialize() const
{
    SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto inputFunc = SerializableFunction();
    inputFunc.CopyFrom(inputFunction.serialize());

    serializedAggregationFunction.mutable_on_fields()->Add()->CopyFrom(inputFunc);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterMedianAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("MedianAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return std::make_shared<MedianAggregationLogicalFunction>(arguments.on.at(0));
}
}
