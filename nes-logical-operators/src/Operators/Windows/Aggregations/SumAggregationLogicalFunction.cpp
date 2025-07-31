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

#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include "DataTypes/DataType.hpp"
#include "DataTypes/DataTypeProvider.hpp"

namespace NES
{
SumAggregationLogicalFunction::SumAggregationLogicalFunction(LogicalFunction inputFunction)
    : WindowAggregationLogicalFunction(std::move(inputFunction))
{
}

SumAggregationLogicalFunction::SumAggregationLogicalFunction(LogicalFunction inputFunction, DataType aggregateType): WindowAggregationLogicalFunction(std::move(aggregateType), std::move(inputFunction))
{
}

std::string_view SumAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}


std::shared_ptr<WindowAggregationLogicalFunction> SumAggregationLogicalFunction::withInferredType(const Schema& schema)
{
    const auto newInputFunction = inputFunction.withInferredDataType(schema);

    const DataType outputType = [&]
    {
        if (newInputFunction.getDataType().isSignedInteger())
        {
            return DataTypeProvider::provideDataType(DataType::Type::INT64);
        }
        if (newInputFunction.getDataType().isInteger())
        {
            return DataTypeProvider::provideDataType(DataType::Type::UINT64);
        }
        if (newInputFunction.getDataType().isFloat())
        {
            return DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
        }
        throw CannotInferStamp("aggregations on non numeric fields is not supported.", newInputFunction.getDataType().isNumeric());
    }();

    SumAggregationLogicalFunction newFunction{inputFunction, outputType};
    return std::make_shared<SumAggregationLogicalFunction>(newFunction);
}

SerializableAggregationFunction SumAggregationLogicalFunction::serialize() const
{
    SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto inputFunc = SerializableFunction();
    inputFunc.CopyFrom(inputFunction.serialize());

    serializedAggregationFunction.mutable_on_fields()->Add()->CopyFrom(inputFunc);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterSumAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if(arguments.on.size() != 1)
    {
        throw CannotDeserialize("SumAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return std::make_shared<SumAggregationLogicalFunction>(arguments.on.at(0));
}
}
