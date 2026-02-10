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
#include <vector>

#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <RawTupleBuffer.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// CRTP Interface that enables InputFormatters to define specialized functions to access fields that the InputFormatter can call directly
/// (templated) without the overhead of a virtual function call (or a lambda function/std::function)
/// Different possible kinds of FieldIndexFunctions(FieldOffsets(index raw data), Internal, ZStdCompressed, Arrow, ...)
/// A FieldIndexFunction may also combine different kinds of field access variants for the different fields of one schema
template <typename Derived>
class FieldIndexFunction
{
public:
    /// Expose the FieldIndexFunction interface functions only to the InputFormatter
    template <InputFormatIndexerType FormatterType>
    friend class InputFormatter;

    friend Derived;

private:
    FieldIndexFunction()
    {
        /// Cannot use Concepts / requires because of the cyclic nature of the CRTP pattern.
        /// The InputFormatter (IFT) guarantees that the reference to AbstractBufferProvider (ABP) outlives the FieldIndexFunction
        static_assert(std::is_constructible_v<Derived>, "Derived class must have a default constructor");
    };

public:
    ~FieldIndexFunction() = default;

    [[nodiscard]] FieldIndex getByteOffsetOfFirstTuple() const
    {
        return static_cast<const Derived*>(this)->applyGetByteOffsetOfFirstTuple();
    }

    /// Returns the offset to the first byte of the last tuple that starts in the buffer (but does not end in it)
    /// A field index function may only know this offset after iterating over (and parsing) the buffer.
    /// Such a field index function must make sure to set 'offsetOfLastTupleDelimiter' during the iteration process
    [[nodiscard]] FieldIndex getByteOffsetOfLastTuple() const { return static_cast<const Derived*>(this)->applyGetByteOffsetOfLastTuple(); }

    [[nodiscard]] size_t getTotalNumberOfTuples() const { return static_cast<const Derived*>(this)->applyGetTotalNumberOfTuples(); }

    [[nodiscard]] nautilus::val<bool> hasNext(const nautilus::val<uint64_t>& recordIdx, nautilus::val<Derived*> fieldOffsetsPtr) const
    {
        return static_cast<const Derived*>(this)->applyHasNext(recordIdx, fieldOffsetsPtr);
    }

    template <typename IndexerMetaData>
    [[nodiscard]] Record readSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        const nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        nautilus::val<Derived*> fieldIndexFunction) const
    {
        return static_cast<const Derived*>(this)->template applyReadSpanningRecord<IndexerMetaData>(
            projections, recordBufferPtr, recordIndex, metaData, fieldIndexFunction);
    }
};

}
