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
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_concepts.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Runtime::Execution::Synopsis
{
using NES::Runtime::Execution::Aggregation::AggregationFunction;
using NES::Runtime::Execution::Aggregation::AggregationState;
class SampleWithoutReplacementFunction : public AggregationFunction
{
public:
    SampleWithoutReplacementFunction(
        PhysicalTypePtr inputType,
        PhysicalTypePtr resultType,
        std::unique_ptr<Functions::Function> inputFunction,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector,
        uint64_t sampleSize);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider,
        const Nautilus::Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) override;
    Nautilus::Record
    lower(nautilus::val<AggregationState*> aggregationState, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) override;
    void
    reset(nautilus::val<AggregationState*> aggregationState, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~SampleWithoutReplacementFunction() override = default;

private:
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector;
    nautilus::val<uint64_t> sampleSize;
};

}
