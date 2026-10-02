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

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <StreamTableJoin/StreamTableJoinPhysicalOperator.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <PhysicalOperator.hpp>
#include <val_bool.hpp>

namespace NES
{

class AsOfJoinInputPhysicalOperator final : public PhysicalOperatorConcept
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

/// Right-side state of an ASOF join: a B-tree of the right rows in the order (key hash, timestamp, insertion number descending).
/// Each entry is [hash, timestamp, insertion number, equality keys..., right row].
struct AsOfJoinIndex
{
    /// The equality keys, cast to a type common to both sides; empty without equality predicate (every row then has hash 0).
    std::vector<QualifiedUnboundField> keyFields;
    std::vector<PhysicalFunction> leftKeyFunctions;
    std::vector<PhysicalFunction> rightKeyFunctions;
    /// Layout of a whole entry.
    std::shared_ptr<BTreeTupleLayout> entryLayout;
    /// Layout of the three leading sort fields only. The comparator reads entries through it, so it never touches the row.
    std::shared_ptr<BTreeTupleLayout> sortKeyLayout;
};

/// A directional event-time ASOF join. Left tuples produce output after the
/// right watermark makes their predecessor selection final.
///
/// A probe searches the B-tree for the last entry not after (hash, left timestamp): the newest row of that hash not after the left
/// timestamp, and among equal timestamps the first inserted. It then steps back over hash collisions to the first row with equal keys.
/// Rows with equal keys are adjacent and ordered by time, so compaction is a single in-order pass.
class AsOfJoinPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    /// The three leading sort fields of an entry.
    static const Record::RecordFieldIdentifier KEY_HASH_FIELD;
    static const Record::RecordFieldIdentifier ROW_TIMESTAMP_FIELD;
    static const Record::RecordFieldIdentifier ROW_NUMBER_FIELD;

    AsOfJoinPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        AsOfJoinIndex index,
        PendingRowsLayout pendingRows,
        std::shared_ptr<TupleBufferRef> leftInputBufferRef,
        std::shared_ptr<TupleBufferRef> rightInputBufferRef,
        std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
        OriginId outputOriginId,
        std::unique_ptr<TimeFunction> leftTimeFunction,
        std::unique_ptr<TimeFunction> rightTimeFunction);

    AsOfJoinPhysicalOperator(const AsOfJoinPhysicalOperator& other);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;
    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    void processLeftRecord(ExecutionContext& executionCtx, Record& record, const nautilus::val<Timestamp>& rightWatermark) const;
    void processRightRecord(ExecutionContext& executionCtx, Record& record, const nautilus::val<TupleBuffer*>& rightState) const;
    [[nodiscard]] nautilus::val<TupleBuffer*> getRightState(ExecutionContext& executionCtx) const;
    /// Writes the key fields and their hash into `record`; returns whether a key is null.
    [[nodiscard]] nautilus::val<bool>
    writeKeys(ExecutionContext& executionCtx, Record& record, const std::vector<PhysicalFunction>& keyFunctions) const;
    [[nodiscard]] nautilus::val<bool> keysEqual(const Record& lhs, const Record& rhs) const;
    void probeLeftRecord(ExecutionContext& executionCtx, const Record& leftRecord, const nautilus::val<Timestamp>& leftTimestamp) const;
    void emitJoinedRecord(ExecutionContext& executionCtx, const Record& leftRecord, const Record& rightRecord) const;
    void releasePending(
        ExecutionContext& executionCtx, const nautilus::val<Timestamp>& rightWatermark, const nautilus::val<bool>& releaseAll) const;
    void compactRightState(ExecutionContext& executionCtx, const nautilus::val<Timestamp>& watermark) const;

    OperatorHandlerId operatorHandlerId;
    AsOfJoinIndex index;
    std::shared_ptr<TupleBufferRef> leftInputBufferRef;
    std::shared_ptr<TupleBufferRef> rightInputBufferRef;
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout;
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout;
    std::vector<Record::RecordFieldIdentifier> leftInputFields;
    std::vector<Record::RecordFieldIdentifier> rightInputFields;
    std::vector<Record::RecordFieldIdentifier> leftFields;
    std::vector<Record::RecordFieldIdentifier> rightFields;
    OriginId outputOriginId;
    std::unique_ptr<TimeFunction> leftTimeFunction;
    std::unique_ptr<TimeFunction> rightTimeFunction;
    /// Compiled in setup(); orders entries by their sort fields.
    mutable std::shared_ptr<BTreeComparator> comparator;
    PendingRowsLayout pendingRows;
    mutable std::shared_ptr<BTreeComparator> pendingComparator;
    std::optional<PhysicalOperator> child;
};

}
