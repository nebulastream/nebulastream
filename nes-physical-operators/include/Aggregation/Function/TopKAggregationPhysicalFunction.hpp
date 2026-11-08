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
#include <memory>
#include <string>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/RowHeap/RowHeapRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Schema/Schema.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// The state of ORDER BY ... LIMIT k BY ...: per key, the top k rows in a RowHeap, ordered by the sort keys and then by their arrival
/// order (sequence number, chunk number and position, see arrivalFields), so equal sort keys keep the earlier row first and every
/// row has a distinct place. The root of the heap is the last of the top rows: a row enters while the heap holds fewer than k
/// rows, or by replacing the root if it comes before it. The heap holds at most k rows, and it compacts the variable-sized values
/// of replaced rows away. The probes read the heap (see UnboundedTopKProbePhysicalOperator and WindowedTopKProbePhysicalOperator);
/// it is not lowered.
class TopKAggregationPhysicalFunction final : public AggregationPhysicalFunction
{
public:
    struct SortKey
    {
        Record::RecordFieldIdentifier field;
        bool descending;
    };

    /// Fields that the build writes into every record before lifting it, in the order in which they break ties.
    static std::vector<Record::RecordFieldIdentifier> arrivalFields();

    /// \p comparatorKey names the compiled comparator of the rows; it must be unique per operator that uses this instance.
    TopKAggregationPhysicalFunction(
        uint64_t limit,
        std::vector<SortKey> sortKeys,
        const Schema<QualifiedUnboundField, Ordered>& inputSchema,
        uint64_t pageSize,
        std::string comparatorKey);

    /// Compiles the comparator of the rows. The operator that uses this instance calls it in its setup.
    void setup(CompilationContext& compilationContext) override;

    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        BorrowedNautilusBuffer parentBuffer1,
        nautilus::val<AggregationState*> aggregationState2,
        BorrowedNautilusBuffer parentBuffer2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(
        nautilus::val<AggregationState*> aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;

    [[nodiscard]] uint64_t getLimit() const { return limit; }
    [[nodiscard]] const std::shared_ptr<BTreeTupleLayout>& getRowLayout() const { return rowLayout; }
    [[nodiscard]] const BTreeComparator& getComparator() const;

    /// The buffer of the heap that holds the rows of a state. Wrap it in a RowHeapRef with getRowLayout().
    [[nodiscard]] static OwnedNautilusBuffer loadRows(const nautilus::val<AggregationState*>& aggregationState, BorrowedNautilusBuffer parentBuffer);

    /// True if row \p first comes strictly before row \p second.
    [[nodiscard]] nautilus::val<bool> comesBefore(const Record& first, const Record& second) const;

private:
    /// Adds the row to the top rows if it belongs to them.
    void insert(const RowHeapRef& rows, const Record& row, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const;

    uint64_t limit;
    /// The sort keys, followed by the arrival fields in ascending order.
    std::vector<SortKey> orderKeys;
    std::shared_ptr<BTreeTupleLayout> rowLayout;
    uint64_t rowSize;
    std::vector<uint64_t> varSizedFieldOffsets;
    uint64_t pageSize;
    std::string comparatorKey;
    /// Compiled in setup; one per compilation, as a comparator belongs to the module it was compiled in.
    mutable std::vector<std::shared_ptr<BTreeComparator>> comparators;
    mutable BTreeComparator* activeComparator = nullptr;
};

}
