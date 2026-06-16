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
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ExecutionContext.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

class NautilusBuffer;

/// Base class for any aggregation state. This class is used to store the intermediate state of an aggregation.
/// For example, the aggregation state for a sum aggregation would be the sum of all values seen so far.
/// For a median aggregation, the aggregation value would be a data structure that stores all seen values so far.
struct AggregationState
{
};

/// A handle to one aggregation's state together with the buffer backing the data structure it lives in. The state is a region of the
/// parent buffer (the hash-map entry's value area): byte 0 holds the null flag when the aggregation is nullable, the value follows.
/// The parent buffer is used to allocate/load the state's variable-sized child buffers (e.g. the median's paged vector). Lightweight,
/// non-owning value type; the referenced NautilusBuffer must outlive the handle.
class AggregationStateRef
{
public:
    AggregationStateRef(nautilus::val<AggregationState*> state, NautilusBuffer& buffer);

    /// Pointer to the start of the state (byte 0 is the null flag when the aggregation is nullable).
    [[nodiscard]] nautilus::val<int8_t*> data() const;

    /// Pointer to the value area, skipping the leading null byte when @p hasNullByte.
    [[nodiscard]] nautilus::val<int8_t*> valueData(bool hasNullByte) const;

    /// The parent buffer the state lives in; used to allocate/load the state's variable-sized child buffers. Intentionally returns a
    /// mutable reference even from a const handle (the handle is a non-owning view), so reset can storeChild through a const handle.
    [[nodiscard]] NautilusBuffer& getBuffer() const;

    /// Reads/writes the null flag stored in the first byte of the state.
    [[nodiscard]] nautilus::val<bool> readNull() const;
    void storeNull(const nautilus::val<bool>& isNull) const;

private:
    /// Base pointer to the state, materialized once at construction so the accessors reuse a single traced value.
    nautilus::val<int8_t*> base;
    NautilusBuffer* parentBuffer;
};

/// This class represents an aggregation function. An aggregation function is used to aggregate records in a window.
/// It represents a single aggregation operation, e.g., sum, min, max, etc.
/// We take the lift, combine, lower, and reset functions from the paper "General Incremental Sliding-Window Aggregation" by Kanat Tangwongsan et al.
/// We add a reset function to reset the aggregation state to its initial state.
class AggregationPhysicalFunction
{
public:
    AggregationPhysicalFunction(
        DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier);

    /// Adds the incoming record to the existing aggregation state.
    virtual void lift(const AggregationStateRef& aggregationState, PipelineMemoryProvider& bufferProvider, const Record& record) = 0;

    /// Combines two aggregation states into one. After calling this method, aggregationState1 contains the combined state.
    virtual void combine(
        const AggregationStateRef& aggregationState1,
        const AggregationStateRef& aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider)
        = 0;

    /// Returns the aggregation state as a nautilus record. The record will contain the aggregation state in the field specified by resultFieldIdentifier
    /// It will NOT contain any other metadata fields, e.g., window start and end fields
    virtual Record lower(const AggregationStateRef& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) = 0;

    /// Resets the aggregation state to its initial state. For a sum, this would be 0, for a min aggregation, this would be the maximum possible value, etc.
    virtual void reset(const AggregationStateRef& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) = 0;

    /// Destroys the aggregation state. This is used to free up memory when the aggregation state is no longer needed.
    virtual void cleanup(nautilus::val<AggregationState*> aggregationState) = 0;

    /// Returns the size of the aggregation state in bytes
    [[nodiscard]] virtual size_t getSizeOfStateInBytes() const = 0;

    virtual ~AggregationPhysicalFunction();

protected:
    DataType inputType;
    DataType resultType;
    PhysicalFunction inputFunction;
    const Record::RecordFieldIdentifier resultFieldIdentifier;
};

}
