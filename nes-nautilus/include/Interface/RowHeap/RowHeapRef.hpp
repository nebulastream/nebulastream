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
#include <vector>
#include <DataTypes/UnboundField.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/RowHeap/RowHeap.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Schema/Schema.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Record-facing Nautilus interface for RowHeap. Rows are laid out by a BTreeTupleLayout and ordered by a compiled BTreeComparator,
/// whose comparator defines when a row comes before another one; the root of the heap is the row that comes last.
class RowHeapRef
{
public:
    RowHeapRef(NautilusBuffer heapBuffer, std::shared_ptr<BTreeTupleLayout> tupleLayout);

    /// Offsets of the variable-sized fields in a row of DefaultBTreeTupleLayout(schema), for RowHeap::init.
    [[nodiscard]] static std::vector<uint64_t> getVarSizedFieldOffsets(const Schema<QualifiedUnboundField, Ordered>& schema);

    [[nodiscard]] nautilus::val<uint64_t> size() const;
    /// The row at the index in heap order; index 0 is the row that comes last. After sortRows, index 0 is the row that comes first.
    [[nodiscard]] Record at(const nautilus::val<uint64_t>& index) const;

    /// Adds the record; the heap must not be full.
    void push(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider, const BTreeComparator& comparator) const;
    /// Replaces the row that comes last with the record; the heap must be full.
    void
    replaceTop(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider, const BTreeComparator& comparator) const;
    /// Sorts the rows in their order, after which they are only read (see RowHeap::sortRows).
    void sortRows(const BTreeComparator& comparator) const;

private:
    /// The order of the compiled comparator for the rows of the heap in the buffer.
    [[nodiscard]] static RowHeap::Comparator comparatorOf(TupleBuffer* heapBuffer, const BTreeComparator* comparator);
    void writePending(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const;

    /// The heap lives in the buffer's memory, so a const RowHeapRef still writes it.
    mutable NautilusBuffer heapBuffer;
    std::shared_ptr<BTreeTupleLayout> tupleLayout;
};

}
