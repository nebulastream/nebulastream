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
#include <memory>
#include <optional>
#include <DataTypes/UnboundSchema.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// The records of one key of a hash join, which are stored in the value area of the key's entry in the chained hash map.
///
/// The value area has a small buffer for the first N records, as most keys only have a few records (small buffer optimization):
///     [u32 child index of the paged vector for the other records, or NO_OVERFLOW][u32 number of records][N records]
/// Records that do not fit are stored in a paged vector that is created when the first of them arrives. It is a child of the hash map buffer.
/// This saves the buffer of a paged vector for each key with up to N records, and the memory accesses to reach it.
///
/// With N = 0, or for records with var sized data, all records are stored in a paged vector and the value area only holds its child index.
class EntryRecordsRef
{
public:
    static constexpr uint32_t NO_OVERFLOW = UINT32_MAX;

    /// The records of an entry, which has to be initialized.
    /// @param valueMemArea the value area of the entry of the key, see getValueAreaSize
    /// @param inlineRecords N, as returned by getEffectiveInlineRecords
    EntryRecordsRef(
        const BorrowedNautilusBuffer& hashMapBuffer,
        const nautilus::val<int8_t*>& valueMemArea,
        std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
        uint64_t inlineRecords,
        uint64_t pageSize);

    /// The number of inline records that are used for records of the schema. The small buffer is only used for records without var sized
    /// data and small compared to a page, so 0 might be returned for the requested number.
    [[nodiscard]] static uint64_t
    getEffectiveInlineRecords(const Schema<QualifiedUnboundField, Ordered>& schema, uint64_t requestedInlineRecords, uint64_t pageSize);

    /// The size in bytes of the value area of an entry for records of the size `tupleSize`.
    [[nodiscard]] static uint64_t getValueAreaSize(uint64_t inlineRecords, uint64_t tupleSize);

    /// Prepares the value area of a new entry, before records are added or read.
    static void initialize(
        const BorrowedNautilusBuffer& hashMapBuffer,
        const nautilus::val<int8_t*>& valueMemArea,
        const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout,
        uint64_t inlineRecords,
        uint64_t pageSize,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider);

    /// Appends a record.
    void pushBack(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider);

    class Sentinel;

    class Iterator
    {
    public:
        [[nodiscard]] Record operator*() const;
        Iterator& operator++();
        [[nodiscard]] nautilus::val<bool> operator!=(const Sentinel& end) const;

    private:
        friend class EntryRecordsRef;
        Iterator(
            BorrowedNautilusBuffer hashMapBuffer,
            nautilus::val<int8_t*> valueMemArea,
            std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
            uint64_t inlineRecords,
            uint64_t tupleSize,
            std::optional<PagedVectorRefIter> pagedVectorIterator);

        BorrowedNautilusBuffer hashMapBuffer;
        nautilus::val<int8_t*> valueMemArea;
        std::shared_ptr<PagedVectorTupleLayout> tupleLayout;
        uint64_t inlineRecords;
        uint64_t tupleSize;
        nautilus::val<uint64_t> position = 0;
        /// The iterator of the paged vector with all records, if there are no inline records
        std::optional<PagedVectorRefIter> pagedVectorIterator;
    };

    class Sentinel
    {
    public:
        [[nodiscard]] nautilus::val<bool> operator==(const Iterator& other) const { return not(other != *this); }

    private:
        friend class EntryRecordsRef;
        friend class Iterator;

        explicit Sentinel(nautilus::val<uint64_t> numberOfRecords) : numberOfRecords(std::move(numberOfRecords)) { }

        explicit Sentinel(PagedVectorRefIterSentinel pagedVectorSentinel)
            : numberOfRecords(0), pagedVectorSentinel(std::move(pagedVectorSentinel))
        {
        }

        nautilus::val<uint64_t> numberOfRecords;
        std::optional<PagedVectorRefIterSentinel> pagedVectorSentinel;
    };

    [[nodiscard]] Iterator begin() const;
    [[nodiscard]] Sentinel end() const;

private:
    BorrowedNautilusBuffer hashMapBuffer;
    nautilus::val<int8_t*> valueMemArea;
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout;
    uint64_t inlineRecords;
    uint64_t pageSize;
    uint64_t tupleSize;
    /// The records of the key if there are no inline records
    std::optional<PagedVectorRef> pagedVector;
};

}
