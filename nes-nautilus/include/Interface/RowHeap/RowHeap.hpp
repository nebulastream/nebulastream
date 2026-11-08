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

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// A bounded max-heap of fixed-size rows, e.g. the k best rows of a top-k: the row that comes last in the order is at the root,
/// so a better row replaces it. The rows live in `capacity` slots of the main buffer, followed by a scratch slot into which a new
/// row is written before pushPending() or replacePendingTop() moves it into the heap.
///
/// Variable-sized values live in the child buffer with index 0 and are referenced by VariableSizedAccess fields at the given
/// offsets of a row. A replaced row leaves its values behind as garbage. When a value does not fit, the child is compacted into
/// a new buffer with twice the space of the live values, which replaces the child and frees the garbage. Compacting is amortized
/// constant per byte, and the values take at most twice the space of the live ones.
class RowHeap
{
public:
    /// True if the row at the first address comes strictly before the row at the second one.
    using Comparator = std::function<bool(const int8_t* first, const int8_t* second)>;
    static constexpr uint64_t MAX_VAR_SIZED_FIELDS = 64;

    [[nodiscard]] static uint64_t getMainBufferSize(uint64_t capacity, uint64_t rowSize);
    static void init(
        TupleBuffer& buffer,
        AbstractBufferProvider& bufferProvider,
        uint64_t capacity,
        uint64_t rowSize,
        uint64_t varSizedPageSize,
        std::span<const uint64_t> varSizedFieldOffsets);
    [[nodiscard]] static RowHeap load(const TupleBuffer& buffer);

    [[nodiscard]] uint64_t size() const;
    [[nodiscard]] uint64_t capacity() const;
    /// The row at the index in heap order (index 0 is the root, the row that comes last), or the scratch slot at index capacity().
    [[nodiscard]] int8_t* row(uint64_t index) const;

    /// Zeroes the scratch slot, so that its variable-sized fields are empty until they are written.
    [[nodiscard]] int8_t* clearPending();
    /// Copies a variable-sized value of the pending row into the variable-sized data of the heap and returns its access.
    [[nodiscard]] VariableSizedAccess storeVarSized(AbstractBufferProvider& bufferProvider, std::span<const std::byte> value);
    /// Adds the pending row to the heap, which must not be full.
    void pushPending(const Comparator& comesBefore);
    /// Replaces the root, the row that comes last, with the pending row. The heap must be full.
    void replacePendingTop(const Comparator& comesBefore);

    /// Sorts the rows so that row 0 comes first. Afterwards the rows are no longer a heap: only read them, e.g. to emit them in order.
    void sortRows(const Comparator& comesBefore);

    /// Bytes in the variable-sized child, live or garbage, and its capacity.
    [[nodiscard]] uint64_t getVarSizedBytesUsed() const;
    [[nodiscard]] uint64_t getVarSizedCapacity() const;

private:
    struct Header
    {
        uint64_t magic;
        uint64_t capacity;
        uint64_t rowSize;
        uint64_t size;
        uint64_t varSizedUsed;
        uint64_t varSizedCapacity;
        uint64_t varSizedPageSize;
        uint64_t numberOfVarSizedFields;
        std::array<uint64_t, MAX_VAR_SIZED_FIELDS> varSizedFieldOffsets;
    };
    static constexpr uint64_t VALID_ROW_HEAP = 7302159447140621;

    explicit RowHeap(TupleBuffer buffer);
    [[nodiscard]] Header& header() const;
    void swapRows(uint64_t first, uint64_t second);
    void siftUp(uint64_t index, const Comparator& comesBefore);
    void siftDown(uint64_t index, const Comparator& comesBefore);
    /// Moves the live values of the rows and the scratch slot into a new child with room for \p additionalBytes more.
    void compact(AbstractBufferProvider& bufferProvider, uint64_t additionalBytes);

    /// The heap lives in the buffer's memory, so a const RowHeap still writes it.
    mutable TupleBuffer buffer;
};

}
