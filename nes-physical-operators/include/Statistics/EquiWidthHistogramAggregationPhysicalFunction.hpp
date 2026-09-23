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

#pragma once

#include <cstddef>
#include <cstdint>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ExecutionContext.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Counts how many values fall into each of numberOfBins equally wide bins over minValue..maxValue, per window.
///
/// The state is nothing but the counters, numberOfBins * uint64: the bin geometry is the same for every state of a
/// query, so it is not worth storing per state and is only written into the blob lower() emits. That makes reset a
/// memset, combine a loop of additions, and lower a header write plus a memcpy.
///
/// A value outside minValue..maxValue is counted in the LAST bin rather than dropped, and so is maxValue itself.
/// Below minValue this follows from unsigned arithmetic (value - minValue wraps to a huge number, which the clamp
/// catches), above maxValue from the clamp directly. This is deliberate and tested: an out-of-range value stays
/// visible in the histogram instead of silently disappearing, at the cost of a last bin that also means "outside".
/// Type inference has already guaranteed a non-nullable unsigned integer input, so lift neither converts nor has a
/// null to handle.
class EquiWidthHistogramAggregationPhysicalFunction final : public AggregationPhysicalFunction
{
public:
    EquiWidthHistogramAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        uint64_t numberOfBins,
        uint64_t minValue,
        uint64_t maxValue);

    static AggregationPhysicalFunctionRegistryReturnType create(AggregationPhysicalFunctionRegistryArguments arguments);

    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        nautilus::val<TupleBuffer*> parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<TupleBuffer*> parentBuffer1,
        nautilus::val<AggregationState*> aggregationState2,
        nautilus::val<TupleBuffer*> parentBuffer2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<TupleBuffer*> parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<TupleBuffer*> parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~EquiWidthHistogramAggregationPhysicalFunction() override = default;

private:
    uint64_t numberOfBins;
    uint64_t minValue;
    uint64_t maxValue;
    /// (maxValue - minValue) / numberOfBins, at least 1 because the logical function rejects a narrower range.
    uint64_t binWidth;
};

}
