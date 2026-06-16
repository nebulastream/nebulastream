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
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

class MaxAggregationPhysicalFunction : public AggregationPhysicalFunction
{
public:
    MaxAggregationPhysicalFunction(
        DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier);
    void lift(const AggregationStateRef& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record) override;
    void combine(
        const AggregationStateRef& aggregationState1,
        const AggregationStateRef& aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(const AggregationStateRef& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(const AggregationStateRef& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~MaxAggregationPhysicalFunction() override = default;
};

}
