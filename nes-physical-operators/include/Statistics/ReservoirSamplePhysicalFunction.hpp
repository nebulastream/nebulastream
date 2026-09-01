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
#include <memory>
#include <vector>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Statistics/ReservoirSampleBlob.hpp>
#include <ExecutionContext.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Builds a uniform reservoir sample (Algorithm R, Vitter 1985) of at most `sampleSize` whole input records
/// per aggregation state. The sampled records are kept in a PagedVector stored as a child buffer of the parent
/// hash-map buffer (same state model as MedianAggregationPhysicalFunction); lower() serializes the sample into
/// a ReservoirSampleBlob emitted as VARSIZED data, together with the number of seen tuples.
///
/// State layout: [childBufferIndex: uint32][numberOfSeenTuples: uint64].
/// combine() merges two reservoirs statistically exactly (see ReservoirMerge.hpp).
/// Eviction choices use a per-worker-thread RNG, so which records survive a full reservoir is not reproducible
/// across runs; merge selections are deterministic for fixed inputs and seed.
class ReservoirSamplePhysicalFunction final : public AggregationPhysicalFunction
{
public:
    ReservoirSamplePhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
        Record::RecordFieldIdentifier numberOfSeenTuplesFieldIdentifier,
        uint64_t sampleSize,
        uint64_t seed);
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
    ~ReservoirSamplePhysicalFunction() override = default;

private:
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout;
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldIdentifier;
    uint64_t sampleSize;
    uint64_t seed;
    /// Field order and types of the serialized sample tuples, derived from the tuple layout schema
    std::vector<ReservoirSampleField> blobFields;
    /// Sum of the serialized sizes of all fixed-size fields per tuple
    uint64_t fixedFieldsSizePerTuple;
};

}
