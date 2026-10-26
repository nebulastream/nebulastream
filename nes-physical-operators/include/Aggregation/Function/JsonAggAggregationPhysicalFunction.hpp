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
#include <memory>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// JSON_AGG: keeps every input record of the window in a paged vector (a child buffer of the window state, as MEDIAN
/// does) and, on lower, writes the input field of each record, in paged-vector order, as one JSON array of strings into
/// the pipeline arena. Inputs are NOT NULL (checked by the logical function).
class JsonAggAggregationPhysicalFunction : public AggregationPhysicalFunction
{
public:
    JsonAggAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<PagedVectorTupleLayout> tupleLayout);
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
    ~JsonAggAggregationPhysicalFunction() override = default;

    static AggregationPhysicalFunctionRegistryReturnType create(AggregationPhysicalFunctionRegistryArguments arguments);

private:
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout;
};

}
