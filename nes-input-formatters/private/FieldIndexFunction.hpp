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
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Concepts.hpp>

namespace NES
{

using FieldIndex = uint32_t;

/// CRTP Interface that enables InputFormatters to define specialized functions to access fields that the InputFormatterTask can call directly
/// (templated) without the overhead of a virtual function call (or a lambda function/std::function)
/// Different possible kinds of FieldIndexFunctions(FieldOffsets(index raw data), Internal, ZStdCompressed, Arrow, ...)
/// A FieldIndexFunction may also combine different kinds of field access variants for the different fields of one schema
template <typename Derived>
class FieldIndexFunction
{
public:
    /// Expose the FieldIndexFunction interface functions only to the InputFormatterTask
    template <InputFormatIndexerType FormatterType>
    friend class InputFormatterTask;

    friend Derived;

private:
    FieldIndexFunction()
    {
        /// Cannot use Concepts / requires because of the cyclic nature of the CRTP pattern.
        /// The InputFormatterTask (IFT) guarantees that the reference to AbstractBufferProvider (ABP) outlives the FieldIndexFunction
        static_assert(std::is_constructible_v<Derived>, "Derived class must have a default constructor");
    };

public:
    ~FieldIndexFunction() = default;

    [[nodiscard]] FieldIndex getOffsetOfFirstTupleDelimiter() const
    {
        return static_cast<const Derived*>(this)->applyGetOffsetOfFirstTupleDelimiter();
    }

    [[nodiscard]] FieldIndex getOffsetOfLastTupleDelimiter() const
    {
        return static_cast<const Derived*>(this)->applyGetOffsetOfLastTupleDelimiter();
    }

    [[nodiscard]] size_t getTotalNumberOfTuples() const { return static_cast<const Derived*>(this)->applyGetTotalNumberOfTuples(); }

    template <typename IndexerMetaData>
    [[nodiscard]] Nautilus::Record readSpanningRecord(
        const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        nautilus::val<Derived*> fieldIndexFunction,
        ArenaRef& arenaRef) const
    {
        return static_cast<const Derived*>(this)->template applyReadSpanningRecord<IndexerMetaData>(
            projections, recordBufferPtr, recordIndex, metaData, fieldIndexFunction, arenaRef);
    }
};

}
