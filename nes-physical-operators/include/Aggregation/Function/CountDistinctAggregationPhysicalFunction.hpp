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

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// COUNT(DISTINCT x): keeps every non-null value of x once per key. The first `inlineValues` distinct values are stored in the
/// aggregation state itself; only the next one creates a hash set (a chained hash map whose only key is the value), and the stored
/// values move into it. Most keys of a slice see a few values, so they never allocate. Lowering returns the number of values. The set
/// has a fixed number of buckets (count_distinct_number_of_buckets); more distinct values per key make its chains longer.
/// Var-sized values always go to the hash set.
class CountDistinctAggregationPhysicalFunction : public AggregationPhysicalFunction
{
public:
    CountDistinctAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        uint64_t numberOfBuckets,
        uint64_t bufferSize,
        uint64_t inlineValues);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        BorrowedNautilusBuffer parentBuffer1,
        nautilus::val<AggregationState*> aggregationState2,
        BorrowedNautilusBuffer parentBuffer2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(
        nautilus::val<AggregationState*> aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~CountDistinctAggregationPhysicalFunction() override = default;

    static AggregationPhysicalFunctionRegistryReturnType create(AggregationPhysicalFunctionRegistryArguments arguments);

private:
    /// Adds a non-null value to the state, inline if there is room, otherwise to the hash set.
    void addValue(
        const nautilus::val<AggregationState*>& aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const VarVal& value) const;
    /// Creates an empty hash set for a state that has none yet.
    void createValueSet(
        const nautilus::val<AggregationState*>& aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) const;

    /// The hash set of a state: a chained hash map with the value as its only key and no values.
    ChainedHashMapConfig valueSetConfig;
    /// Type of the inline values (the input type without null) and how many fit into the state.
    DataType inlineValueType;
    uint64_t inlineCapacity;
};

}
