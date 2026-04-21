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

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <RawTupleBuffer.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Forward ref to break circular dependency between InputFormatIndexer (which returns RawBufferIndex) and RawBufferIndex.
class InputFormatIndexer;

/// Byte offsets of the first and last tuple delimiter in an indexed buffer. max() means the corresponding delimiter was not found.
struct TupleDelimiterOffsets
{
    FieldIndex first{};
    FieldIndex last{};
};

/// Created by 'InputFormatIndexer::indexRawBuffer'. Represents an indexed view over a raw buffer that the
/// InputFormatter can iterate over.
class RawBufferIndex
{
public:
    RawBufferIndex() = default;
    virtual ~RawBufferIndex() = default;

    /// Traced by the InputFormatter. Passing 'rawBufferIndex' as a nautilus value is necessary to bake the 'this' address of
    /// the RawBufferIndex used during tracing into the compiled code.
    [[nodiscard]] virtual nautilus::val<bool>
    hasNext(const nautilus::val<uint64_t>& tupleIdx, const nautilus::val<RawBufferIndex*>& rawBufferIndex) const = 0;

    /// Create a record based on the underlying index and return it.
    /// Traced by the InputFormatter. Passing 'rawBufferIndex' as a nautilus value is necessary to bake the 'this' address of
    /// the RawBufferIndex used during tracing into the compiled code.
    [[nodiscard]] virtual Record readSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        const nautilus::val<uint64_t>& recordIndex,
        const InputFormatIndexer& indexer,
        nautilus::val<RawBufferIndex*> rawBufferIndex,
        const TupleBufferRef& bufferRef) const
        = 0;

    [[nodiscard]] virtual TupleDelimiterOffsets getTupleDelimiterOffsets() const = 0;
    [[nodiscard]] virtual size_t getNumberOfTuples() const = 0;
};

}
