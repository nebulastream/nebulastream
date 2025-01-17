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
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationValue.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_concepts.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Runtime::Execution::Aggregation
{
/**
 * This class is the Nautilus aggregation interface
 */
class AggregationFunction
{
public:
    AggregationFunction(
        PhysicalTypePtr inputType,
        PhysicalTypePtr resultType,
        std::unique_ptr<Functions::Function> inputFunction,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier);

    /// Adds the incoming record to the existing aggregation state
    virtual void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider,
        const Nautilus::Record& record)
        = 0;

    /// Combines two aggregation states into one. After calling this method, aggregationState1 will contain the combined state
    virtual void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
        = 0;

    /// Returns the aggregation state as a nautilus record. The record will contain the aggregation state in the field specified by resultFieldIdentifier
    /// It will NOT contain any other metadata fields, e.g., window start and end fields
    virtual Nautilus::Record
    lower(nautilus::val<AggregationState*> aggregationState, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
        = 0;

    /// Resets the aggregation state to its initial state. For a sum, this would be 0, for a min aggregation, this would be the maximum possible value, etc.
    virtual void
    reset(nautilus::val<AggregationState*> aggregationState, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
        = 0;

    /// Returns the size of the aggregation state in bytes
    [[nodiscard]] virtual size_t getStateSizeInBytes() const = 0;

    virtual ~AggregationFunction();

protected:
    const PhysicalTypePtr inputType;
    const PhysicalTypePtr resultType;
    const std::unique_ptr<Functions::Function> inputFunction;
    const Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier;
};

}
