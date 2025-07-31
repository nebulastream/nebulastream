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

#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>

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
MaxAggregationLogicalFunction::MaxAggregationLogicalFunction(LogicalFunction inputFunction)
    : WindowAggregationLogicalFunction(inputFunction.getDataType(), std::move(inputFunction))
{
}

std::string_view MaxAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

std::shared_ptr<WindowAggregationLogicalFunction> MaxAggregationLogicalFunction::withInferredType(const Schema& schema)
{
    auto newInputFunction = inputFunction.withInferredDataType(schema);

    if (!newInputFunction.getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot calculate maximum value on non-numeric function {}.", newInputFunction);
    }
    return std::make_shared<MaxAggregationLogicalFunction>(newInputFunction);
}

SerializableAggregationFunction MaxAggregationLogicalFunction::serialize() const
{
    SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto inputFunc = SerializableFunction();
    inputFunc.CopyFrom(inputFunction.serialize());

    serializedAggregationFunction.mutable_on_fields()->Add()->CopyFrom(inputFunc);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterMaxAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("MaxAggregationLogicalFunction requires exactly one fields, but got {}", arguments.on.size());
    }
    return std::make_shared<MaxAggregationLogicalFunction>(arguments.on.at(0));
}
}
