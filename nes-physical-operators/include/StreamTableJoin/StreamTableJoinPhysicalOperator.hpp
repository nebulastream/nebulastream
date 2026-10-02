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
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <PhysicalOperator.hpp>
#include <val_bool.hpp>

namespace NES
{

/// Preserves the typed pipeline boundary for one join input. Finalization is deferred to the merged join pipeline so that one input's
/// termination cannot overtake data still in flight from another input.
class StreamTableJoinInputPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::optional<PhysicalOperator> child;
};

/// Hash index on the equi-join keys: the table rows are stored in a chained hash map (one entry per row, keyed by the equi-join keys
/// cast to a common type), and a stream record only visits the rows with equal keys.
struct StreamTableJoinIndex
{
    ChainedHashMapConfig hashMapConfig;
    /// One function per key field of hashMapConfig, in the same order, evaluated on a stream or on a table record.
    std::vector<PhysicalFunction> streamKeyFunctions;
    std::vector<PhysicalFunction> tableKeyFunctions;
};

/// Pending stream rows of a stream-table or ASOF join: B-tree entries [timestamp, stream row], ordered by timestamp.
struct PendingRowsLayout
{
    std::shared_ptr<BTreeTupleLayout> entryLayout;
    /// The leading timestamp only; the comparator reads entries through it.
    std::shared_ptr<BTreeTupleLayout> sortKeyLayout;
};

/// Name of the timestamp field in front of a pending row.
const Record::RecordFieldIdentifier& pendingTimestampField();

std::shared_ptr<BTreeComparator>
createPendingRowsComparator(CompilationContext& compilationContext, const PendingRowsLayout& layout, OperatorId operatorId);

/// Adds `row` (it gains the timestamp field) to the pending rows. Expects the handler lock to be held.
void appendPendingRow(
    ExecutionContext& executionCtx,
    OperatorHandlerId operatorHandlerId,
    const PendingRowsLayout& layout,
    const BTreeComparator& comparator,
    Record& row,
    const nautilus::val<Timestamp>& timestamp);

/// Calls `probe` on every pending row that `watermark` makes final (all rows with `releaseAll`); the others stay pending. Shared by the
/// stream-table and the ASOF join, whose handlers keep the pending rows. Expects the handler lock to be held.
void releasePendingRows(
    ExecutionContext& executionCtx,
    OperatorHandlerId operatorHandlerId,
    const PendingRowsLayout& layout,
    const BTreeComparator& comparator,
    const nautilus::val<Timestamp>& watermark,
    const nautilus::val<bool>& releaseAll,
    const std::function<void(const Record&, const nautilus::val<Timestamp>&)>& probe);

/// A single origin-aware asymmetric stream-table join scan.
///
/// Buffers from both predecessors enter this operator. Their origin identifies
/// whether the stream or table layout is used. Table buffers grow the retained
/// state and advance its watermark; stream buffers either probe that state or
/// wait until the table watermark is sufficiently advanced. Once the table watermark is
/// infinite, the table is complete and read-only: stream buffers probe it without the lock.
class StreamTableJoinPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    StreamTableJoinPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        /// Evaluated on each candidate pair: the residual if there is an index, else the whole join function. nullopt accepts every pair.
        std::optional<PhysicalFunction> joinFunction,
        std::optional<StreamTableJoinIndex> index,
        PendingRowsLayout pendingRows,
        std::shared_ptr<TupleBufferRef> streamInputBufferRef,
        std::shared_ptr<TupleBufferRef> tableInputBufferRef,
        std::shared_ptr<PagedVectorTupleLayout> streamTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> tableTupleLayout,
        OriginId outputOriginId,
        ///NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
        std::unique_ptr<TimeFunction> streamTimeFunction = nullptr,
        ///NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
        std::unique_ptr<TimeFunction> tableTimeFunction = nullptr);

    StreamTableJoinPhysicalOperator(const StreamTableJoinPhysicalOperator& other);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;
    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    void processStreamRecord(ExecutionContext& executionCtx, Record& record, const nautilus::val<Timestamp>& tableWatermark) const;
    void processTableRecord(ExecutionContext& executionCtx, Record& record, const nautilus::val<TupleBuffer*>& tableState) const;
    [[nodiscard]] nautilus::val<TupleBuffer*> getTableState(ExecutionContext& executionCtx) const;
    [[nodiscard]] std::pair<Record, nautilus::val<bool>>
    computeKeys(ExecutionContext& executionCtx, const Record& record, const std::vector<PhysicalFunction>& keyFunctions) const;
    void
    probeStreamRecord(ExecutionContext& executionCtx, const Record& streamRecord, const nautilus::val<Timestamp>& streamTimestamp) const;
    void emitJoinedRecord(
        ExecutionContext& executionCtx,
        const Record& streamRecord,
        Record& tableRecord,
        const nautilus::val<Timestamp>& streamTimestamp) const;
    void releasePending(
        ExecutionContext& executionCtx, const nautilus::val<Timestamp>& tableWatermark, const nautilus::val<bool>& releaseAll) const;

    OperatorHandlerId operatorHandlerId;
    std::optional<PhysicalFunction> joinFunction;
    std::optional<StreamTableJoinIndex> index;
    std::shared_ptr<TupleBufferRef> streamInputBufferRef;
    std::shared_ptr<TupleBufferRef> tableInputBufferRef;
    std::shared_ptr<PagedVectorTupleLayout> streamTupleLayout;
    std::shared_ptr<PagedVectorTupleLayout> tableTupleLayout;
    std::vector<Record::RecordFieldIdentifier> streamInputFields;
    std::vector<Record::RecordFieldIdentifier> tableInputFields;
    std::vector<Record::RecordFieldIdentifier> streamFields;
    std::vector<Record::RecordFieldIdentifier> tableFields;
    OriginId outputOriginId;
    std::unique_ptr<TimeFunction> streamTimeFunction;
    std::unique_ptr<TimeFunction> tableTimeFunction;
    PendingRowsLayout pendingRows;
    /// Compiled in setup().
    mutable std::shared_ptr<BTreeComparator> pendingComparator;
    std::optional<PhysicalOperator> child;
};

}
