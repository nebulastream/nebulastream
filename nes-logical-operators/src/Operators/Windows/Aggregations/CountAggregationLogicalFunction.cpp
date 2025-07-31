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

#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
CountAggregationLogicalFunction::CountAggregationLogicalFunction(LogicalFunction inputFunction)
    : WindowAggregationLogicalFunction(DataTypeProvider::provideDataType(finalAggregateStampType), std::move(inputFunction))
{
}

std::string_view CountAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

std::shared_ptr<WindowAggregationLogicalFunction> CountAggregationLogicalFunction::withInferredType(const Schema& schema)
{
    auto newInputFunction = inputFunction.withInferredDataType(schema);
    return std::make_shared<CountAggregationLogicalFunction>(newInputFunction);
}

SerializableAggregationFunction CountAggregationLogicalFunction::serialize() const
{
    SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto inputFunc = SerializableFunction();
    inputFunc.CopyFrom(inputFunction.serialize());

    serializedAggregationFunction.mutable_on_fields()->Add()->CopyFrom(inputFunc);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterCountAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("CountAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return std::make_shared<CountAggregationLogicalFunction>(arguments.on.at(0));
}
}
