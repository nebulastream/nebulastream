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
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <val_concepts.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES
{

class CountAggregationFunction : public AggregationFunction
{
public:
    CountAggregationFunction(
        std::unique_ptr<PhysicalType> inputType,
        std::unique_ptr<PhysicalType> resultType,
        Functions::PhysicalFunction inputFunction,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Nautilus::Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Nautilus::Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~CountAggregationFunction() override = default;
};

}
