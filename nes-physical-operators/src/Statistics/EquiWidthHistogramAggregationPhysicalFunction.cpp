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

#include <Statistics/EquiWidthHistogramAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Operators/Statistic/EquiWidthHistogramBlobLayout.hpp>
#include <Operators/Windows/Aggregations/EquiWidthHistogramAggregationLogicalFunction.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Statistics/EquiWidthHistogramBlob.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val_arith.hpp>
#include <val_memcpy.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
nautilus::val<int8_t*> countersOf(const nautilus::val<AggregationState*>& aggregationState)
{
    return static_cast<nautilus::val<int8_t*>>(aggregationState);
}

nautilus::val<int8_t*> counterAt(const nautilus::val<int8_t*>& counters, const nautilus::val<uint64_t>& binIndex)
{
    return counters + (binIndex * nautilus::val<uint64_t>{EquiWidthHistogramBlob::COUNTER_SIZE});
}
}

EquiWidthHistogramAggregationPhysicalFunction::EquiWidthHistogramAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const uint64_t numberOfBins,
    const uint64_t minValue,
    const uint64_t maxValue)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , numberOfBins(numberOfBins)
    , minValue(minValue)
    , maxValue(maxValue)
    , binWidth(equiWidthHistogramBinWidth(minValue, maxValue, numberOfBins))
{
    PRECONDITION(binWidth > 0, "A bin has to be at least one value wide, which the logical function guarantees");
}

void EquiWidthHistogramAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    nautilus::val<TupleBuffer*>,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    /// The input is a non-nullable unsigned integer, so this is the value itself, not a conversion of it.
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena).getRawValueAs<nautilus::val<uint64_t>>();

    /// Below minValue the subtraction wraps to a huge number and above maxValue the quotient runs past the last bin;
    /// both land in the last bin, which therefore also means "outside the range". See the class documentation.
    const auto binIndex = (value - nautilus::val<uint64_t>{minValue}) / nautilus::val<uint64_t>{binWidth};
    const auto lastBin = nautilus::val<uint64_t>{numberOfBins - 1};
    const auto boundedBinIndex = binIndex < nautilus::val<uint64_t>{numberOfBins} ? binIndex : lastBin;

    const auto counter = counterAt(countersOf(aggregationState), boundedBinIndex);
    VarVal{readValueFromMemRef<uint64_t>(counter) + nautilus::val<uint64_t>{1}}.writeToMemory(counter);
}

void EquiWidthHistogramAggregationPhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1,
    nautilus::val<TupleBuffer*>,
    nautilus::val<AggregationState*> aggregationState2,
    nautilus::val<TupleBuffer*>,
    PipelineMemoryProvider&)
{
    /// A runtime loop rather than an unrolled one: with counters only this is a single add per bin, and unrolling a
    /// histogram of hundreds of bins into the trace costs more than it saves.
    const auto counters1 = countersOf(aggregationState1);
    const auto counters2 = countersOf(aggregationState2);
    for (nautilus::val<uint64_t> bin = 0; bin < nautilus::val<uint64_t>{numberOfBins}; bin = bin + 1)
    {
        const auto counter1 = counterAt(counters1, bin);
        const auto combined = readValueFromMemRef<uint64_t>(counter1) + readValueFromMemRef<uint64_t>(counterAt(counters2, bin));
        VarVal{combined}.writeToMemory(counter1);
    }
}

Record EquiWidthHistogramAggregationPhysicalFunction::lower(
    nautilus::val<AggregationState*> aggregationState, nautilus::val<TupleBuffer*>, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto countersSize = nautilus::val<uint64_t>{numberOfBins * EquiWidthHistogramBlob::COUNTER_SIZE};
    const auto blobSize = nautilus::val<uint64_t>{EquiWidthHistogramBlob::HEADER_SIZE} + countersSize;
    const auto blobMemory = pipelineMemoryProvider.arena.allocateMemory(blobSize);

    /// The state is already the blob's payload, so serializing is the header plus one copy.
    writeEquiWidthHistogramBlobHeader(
        blobMemory, nautilus::val<uint64_t>{numberOfBins}, nautilus::val<uint64_t>{minValue}, nautilus::val<uint64_t>{maxValue});
    nautilus::memcpy(getEquiWidthHistogramBlobCounters(blobMemory), countersOf(aggregationState), countersSize);

    Record record;
    record.write(resultFieldIdentifier, VarVal{VariableSizedData{blobMemory, blobSize}});
    return record;
}

void EquiWidthHistogramAggregationPhysicalFunction::reset(
    nautilus::val<AggregationState*> aggregationState, nautilus::val<TupleBuffer*>, PipelineMemoryProvider&)
{
    nautilus::memset(countersOf(aggregationState), 0, getSizeOfStateInBytes());
}

void EquiWidthHistogramAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
    /// The counters live in the hash map entry; there is nothing else to free.
}

size_t EquiWidthHistogramAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return numberOfBins * EquiWidthHistogramBlob::COUNTER_SIZE;
}

AggregationPhysicalFunctionRegistryReturnType
EquiWidthHistogramAggregationPhysicalFunction::create(AggregationPhysicalFunctionRegistryArguments arguments)
{
    const auto histogram = arguments.logicalFunction.tryGetAs<EquiWidthHistogramAggregationLogicalFunction>();
    INVARIANT(histogram.has_value(), "Expected an equi-width histogram aggregation, but got {}", arguments.logicalFunction->getName());

    return std::make_shared<EquiWidthHistogramAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        (*histogram)->getNumberOfBins(),
        (*histogram)->getMinValue(),
        (*histogram)->getMaxValue());
}

}
