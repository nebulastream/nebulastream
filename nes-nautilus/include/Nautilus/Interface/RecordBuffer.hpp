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

#include <cstdint>
#include <ranges>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_std.hpp>

namespace NES
{

/// @brief The RecordBuffer is a representation of a set of records that are stored together.
/// In the common case this maps to a TupleBuffer, which stores the individual records in either a row or a columnar layout.
class RecordBuffer
{
public:
    /// @brief Creates a new record buffer with a reference to a tuple buffer
    /// @param tupleBufferRef
    explicit RecordBuffer(const nautilus::val<TupleBuffer*>& tupleBufferRef);

    void setNumRecords(const nautilus::val<uint64_t>& numRecordsValue);
    [[nodiscard]] nautilus::val<uint64_t> getNumRecords() const;

    /// Retrieve the reference to the underling memory area from the record buffer.
    [[nodiscard]] nautilus::val<int8_t*> getMemArea() const;

    /// Get the reference to the underlying TupleBuffer
    const nautilus::val<TupleBuffer*>& getReference() const;

    /// Get the origin ID of the underlying tuple buffer. The origin ID is a unique identifier for the origin of the tuple buffer.
    nautilus::val<OriginId> getOriginId();
    void setOriginId(const nautilus::val<OriginId>& originId);

    ///  Get the watermark timestamp of the underlying tuple buffer. The watermark timestamp is a point in time that guarantees no records
    ///  with a lower timestamp will be received.
    nautilus::val<Timestamp> getWatermarkTs();
    void setWatermarkTs(const nautilus::val<Timestamp>& watermarkTs);

    /// Get the creation timestamp of the underlying tuple buffer. The creation timestamp is the point in time when the tuple buffer was created.
    nautilus::val<Timestamp> getCreatingTs();
    void setCreationTs(const nautilus::val<Timestamp>& creationTs);

    ~RecordBuffer() = default;

    class SequenceRangeRef
    {
        nautilus::val<uint64_t*> seqStartData = nullptr;
        nautilus::val<uint64_t*> seqEndData = nullptr;
        nautilus::val<size_t> size = 0;

        SequenceRangeRef(
            const nautilus::val<uint64_t*>& seq_start_data,
            const nautilus::val<uint64_t*>& seq_end_data,
            const nautilus::val<size_t>& size);

    public:
        SequenceRangeRef() = default;
        static SequenceRangeRef from(nautilus::val<SequenceRange*> allocated);
        void store(nautilus::val<SequenceRange*> allocated);


        void setStart(const nautilus::val<size_t>& index, const nautilus::val<size_t>& value);
        void setEnd(nautilus::val<size_t> index, nautilus::val<size_t> value);
        [[nodiscard]] nautilus::val<uint64_t> getStart(nautilus::val<size_t> index) const;
        [[nodiscard]] nautilus::val<uint64_t> getEnd(nautilus::val<size_t> index) const;


        [[nodiscard]] nautilus::val<uint64_t*> getStartPointer() const;
        [[nodiscard]] nautilus::val<uint64_t*> getEndPointer() const;
        [[nodiscard]] nautilus::val<size_t> getSize() const;
    };

    SequenceRangeRef getSequenceRange() const;
    void setSequenceRange(SequenceRangeRef rangeRef);

private:
    nautilus::val<TupleBuffer*> tupleBufferRef;
};

}
