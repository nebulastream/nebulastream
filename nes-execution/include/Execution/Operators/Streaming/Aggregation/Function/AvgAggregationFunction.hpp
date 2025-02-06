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
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_concepts.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Runtime::Execution::Aggregation
{

class AvgAggregationFunction : public AggregationFunction
{
public:
    AvgAggregationFunction(
        std::shared_ptr<PhysicalType> inputType,
        std::shared_ptr<PhysicalType> resultType,
        std::unique_ptr<Functions::Function> inputFunction,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<PhysicalType> countType);

    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        const PipelineMemory& pipelineMemory,
        const Nautilus::Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        const PipelineMemory& pipelineMemory) override;
    Nautilus::Record lower(nautilus::val<AggregationState*> aggregationState, const PipelineMemory& pipelineMemory) override;
    void reset(nautilus::val<AggregationState*> aggregationState, const PipelineMemory& pipelineMemory) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~AvgAggregationFunction() override = default;

private:
    std::shared_ptr<PhysicalType> countType;
};

}
