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
#include <string>
#include <vector>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <val_base.hpp>
#include <val_ptr.hpp>
#include "ExecutionContext.hpp"
#include "Interface/PagedVector/PagedVectorComparator.hpp"
#include "Interface/TimestampRef.hpp"
#include "Runtime/TupleBuffer.hpp"
#include "Time/Timestamp.hpp"

namespace NES
{

class ArrayAggAggregationPhysicalFunction final : public AggregationPhysicalFunction
{
public:
    ArrayAggAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
        ///NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
        bool sortByTimestamp = true);

    void setup(CompilationContext& compilationContext) override;
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        nautilus::val<TupleBuffer*> parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record,
        const nautilus::val<Timestamp>& timestamp,
        const AggregationInputBuffer& inputBuffer) override;
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

private:
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout;
    Record::RecordFieldIdentifier timestampFieldIdentifier;
    Record::RecordFieldIdentifier inputFieldIdentifier;
    bool sortByTimestamp;
    std::string comparatorIdentifier;
    std::vector<std::shared_ptr<PagedVectorComparator>> comparatorOwners;
};
}
