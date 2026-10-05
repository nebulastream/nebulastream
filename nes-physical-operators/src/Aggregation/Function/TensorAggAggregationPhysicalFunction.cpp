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

#include <Aggregation/Function/TensorAggAggregationPhysicalFunction.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/TimestampRef.hpp>
#include <Operators/Windows/Aggregations/TensorAggAggregationLogicalFunction.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/function.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_memcpy.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
struct TensorAggState
{
    /// numberOfRows * rowWidth float32 values, row-major.
    int8_t* values = nullptr;
    /// One flag per row; non-zero once a tuple wrote the row.
    int8_t* written = nullptr;
};

void allocateTensor(
    TensorAggState* state,
    TupleBuffer* parentBuffer,
    AbstractBufferProvider* bufferProvider,
    const uint64_t numberOfRows,
    const uint64_t rowWidth,
    const float defaultValue)
{
    PRECONDITION(state != nullptr, "TENSOR_AGG state must not be null");
    PRECONDITION(parentBuffer != nullptr, "TENSOR_AGG parent buffer must not be null");
    PRECONDITION(bufferProvider != nullptr, "TENSOR_AGG buffer provider must not be null");
    const auto numberOfValues = numberOfRows * rowWidth;
    auto tensor = bufferProvider->getUnpooledBuffer(numberOfValues * sizeof(float) + numberOfRows);
    if (not tensor)
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer of {} values available for TENSOR_AGG", numberOfValues);
    }
    auto* const memory = reinterpret_cast<int8_t*>(tensor->getAvailableMemoryArea<>().data()); /// NOLINT
    std::fill_n(reinterpret_cast<float*>(memory), numberOfValues, defaultValue); /// NOLINT
    auto* const written = memory + numberOfValues * sizeof(float); /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    std::fill_n(written, numberOfRows, int8_t{0});
    std::ignore = parentBuffer->storeChildBuffer(*tensor);
    new (state) TensorAggState{.values = memory, .written = written}; /// NOLINT(cppcoreguidelines-owning-memory)
}

void combineTensors(TensorAggState* destination, const TensorAggState* source, const uint64_t numberOfRows, const uint64_t rowBytes)
{
    PRECONDITION(destination != nullptr && source != nullptr, "TENSOR_AGG states must not be null");
    for (uint64_t row = 0; row < numberOfRows; ++row)
    {
        if (source->written[row] != 0) /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        {
            std::memcpy(destination->values + row * rowBytes, source->values + row * rowBytes, rowBytes); /// NOLINT
            destination->written[row] = 1; /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        }
    }
}
}

TensorAggAggregationPhysicalFunction::TensorAggAggregationPhysicalFunction(
    DataType resultType,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::vector<PhysicalFunction> valueFunctions,
    std::vector<DataType> valueTypes,
    std::vector<PhysicalFunction> indexFunctions,
    std::vector<DataType> indexTypes,
    std::vector<uint64_t> indexDimensions,
    const float defaultValue)
    : AggregationPhysicalFunction(valueTypes.front(), std::move(resultType), valueFunctions.front(), std::move(resultFieldIdentifier))
    , valueFunctions(std::move(valueFunctions))
    , valueTypes(std::move(valueTypes))
    , indexFunctions(std::move(indexFunctions))
    , indexTypes(std::move(indexTypes))
    , indexDimensions(std::move(indexDimensions))
    , defaultValue(defaultValue)
    , numberOfRows(std::accumulate(this->indexDimensions.begin(), this->indexDimensions.end(), uint64_t{1}, std::multiplies<>()))
{
    PRECONDITION(this->valueFunctions.size() == this->valueTypes.size(), "TENSOR_AGG needs a type for every value function");
    PRECONDITION(
        this->indexFunctions.size() == this->indexTypes.size() && this->indexFunctions.size() == this->indexDimensions.size(),
        "TENSOR_AGG needs a type and a dimension for every index function");
}

void TensorAggAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record,
    const nautilus::val<Timestamp>&,
    const AggregationInputBuffer&)
{
    /// The row of this tuple, in row-major order over the leading axes, and whether every index lies inside the shape.
    /// Tuples outside the shape or with a NULL index are ignored. The loops over axes and values are unrolled while
    /// tracing; their counters are static_vals so that the branches of different iterations are distinct trace points
    /// (with plain counters, Nautilus mistakes the repeated branch for a loop: "Invalid trace ... constant loop").
    nautilus::val<uint64_t> row{0};
    nautilus::val<bool> insideTensor{true};
    for (nautilus::static_val<size_t> axis = 0; axis < indexFunctions.size(); ++axis)
    {
        const auto index = indexFunctions[axis].execute(record, pipelineMemoryProvider.arena);
        const auto signedPosition = index.getRawValueAs<nautilus::val<int64_t>>();
        insideTensor = insideTensor && signedPosition >= nautilus::val<int64_t>{0}
            && signedPosition < nautilus::val<int64_t>{static_cast<int64_t>(indexDimensions[axis])};
        if (indexTypes[axis].nullable)
        {
            insideTensor = insideTensor && not index.isNull();
        }
        row = row * nautilus::val<uint64_t>{indexDimensions[axis]} + index.getRawValueAs<nautilus::val<uint64_t>>();
    }

    if (insideTensor)
    {
        const auto state = static_cast<nautilus::val<int8_t*>>(aggregationState);
        const auto values = readValueFromMemRef<int8_t*>(getMemberRef(state, &TensorAggState::values));
        const auto written = readValueFromMemRef<int8_t*>(getMemberRef(state, &TensorAggState::written));
        const auto rowStart = values + row * nautilus::val<uint64_t>{valueFunctions.size() * sizeof(float)};
        for (nautilus::static_val<size_t> column = 0; column < valueFunctions.size(); ++column)
        {
            const auto value = valueFunctions[column].execute(record, pipelineMemoryProvider.arena);
            const auto target = rowStart + nautilus::val<uint64_t>{column * sizeof(float)};
            if (valueTypes[column].nullable)
            {
                if (not value.isNull())
                {
                    VarVal{value.getRawValueAs<nautilus::val<float>>()}.writeToMemory(target);
                }
            }
            else
            {
                VarVal{value.getRawValueAs<nautilus::val<float>>()}.writeToMemory(target);
            }
        }
        VarVal{nautilus::val<int8_t>{1}}.writeToMemory(written + row);
    }
}

void TensorAggAggregationPhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer,
    nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        combineTensors,
        static_cast<nautilus::val<TensorAggState*>>(aggregationState1),
        static_cast<nautilus::val<TensorAggState*>>(aggregationState2),
        nautilus::val<uint64_t>{numberOfRows},
        nautilus::val<uint64_t>{valueFunctions.size() * sizeof(float)});
}

Record TensorAggAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, BorrowedNautilusBuffer, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto state = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto values = readValueFromMemRef<int8_t*>(getMemberRef(state, &TensorAggState::values));
    const auto tensorBytes = nautilus::val<uint64_t>{numberOfRows * valueFunctions.size() * sizeof(float)};
    auto payload = pipelineMemoryProvider.arena.allocateVariableSizedData(tensorBytes);
    nautilus::memcpy(payload.getContent(), values, tensorBytes);
    Record result;
    result.write(resultFieldIdentifier, VarVal{payload});
    return result;
}

void TensorAggAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    nautilus::invoke(
        allocateTensor,
        static_cast<nautilus::val<TensorAggState*>>(aggregationState),
        parentBuffer.asArg(),
        pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{numberOfRows},
        nautilus::val<uint64_t>{valueFunctions.size()},
        nautilus::val<float>{defaultValue});
}

void TensorAggAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
    /// The tensor is a child of the parent hash-map buffer and is released with its parent.
}

size_t TensorAggAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(TensorAggState);
}

AggregationPhysicalFunctionRegistryReturnType
TensorAggAggregationPhysicalFunction::create(AggregationPhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.logicalFunction.has_value(), "TENSOR_AGG requires its logical function to be lowered");
    const auto logical = arguments.logicalFunction->getAs<TensorAggAggregationLogicalFunction>();
    const auto numberOfValues = logical->getValueFunctions().size();
    const auto numberOfIndices = logical->getIndexFunctions().size();
    PRECONDITION(
        arguments.additionalInputFunctions.size() == numberOfValues - 1 + numberOfIndices
            && arguments.additionalInputTypes.size() == arguments.additionalInputFunctions.size(),
        "TENSOR_AGG expects its value and index fields as inputs");

    std::vector<PhysicalFunction> valueFunctions{std::move(arguments.inputFunction)};
    std::vector<DataType> valueTypes{std::move(arguments.inputType)};
    std::vector<PhysicalFunction> indexFunctions;
    std::vector<DataType> indexTypes;
    for (size_t input = 0; input < arguments.additionalInputFunctions.size(); ++input)
    {
        const bool isValue = input < numberOfValues - 1;
        (isValue ? valueFunctions : indexFunctions).push_back(std::move(arguments.additionalInputFunctions[input]));
        (isValue ? valueTypes : indexTypes).push_back(std::move(arguments.additionalInputTypes[input]));
    }

    const auto& shape = logical->getShape();
    std::vector<uint64_t> indexDimensions(shape.begin(), shape.begin() + static_cast<std::ptrdiff_t>(numberOfIndices));
    return std::make_shared<TensorAggAggregationPhysicalFunction>(
        std::move(arguments.resultType),
        std::move(arguments.resultFieldIdentifier),
        std::move(valueFunctions),
        std::move(valueTypes),
        std::move(indexFunctions),
        std::move(indexTypes),
        std::move(indexDimensions),
        logical->getDefaultValue());
}
}
