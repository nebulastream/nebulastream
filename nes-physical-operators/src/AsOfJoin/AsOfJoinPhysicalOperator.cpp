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

#include <AsOfJoin/AsOfJoinPhysicalOperator.hpp>
#include <DataTypes/UnboundSchema.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
#include <StreamTableJoin/StreamTableJoinPhysicalOperator.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <fmt/format.h>
#include <nautilus/select.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

namespace
{
void lockHandler(OperatorHandler* handler)
{
    dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).lock();
}

void unlockHandler(OperatorHandler* handler)
{
    dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).unlock();
}

TupleBuffer* getRightTree(OperatorHandler* handler, AbstractBufferProvider* bufferProvider)
{
    return dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).getOrCreateTableIndex(bufferProvider);
}

bool isRightSealed(OperatorHandler* handler)
{
    return dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).isTableSealed();
}

uint64_t getRightWatermark(OperatorHandler* handler)
{
    return dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).getTableWatermark().getRawValue();
}

nautilus::val<uint64_t> readUInt64(const Record& record, const Record::RecordFieldIdentifier& field)
{
    return record.read(field).getRawValueAs<nautilus::val<uint64_t>>();
}


}

void AsOfJoinInputPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    executeChild(executionCtx, record);
}

void AsOfJoinInputPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    closeChild(executionCtx, recordBuffer);
}

void AsOfJoinInputPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    terminateChild(executionCtx);
}

std::optional<PhysicalOperator> AsOfJoinInputPhysicalOperator::getChild() const
{
    return child;
}

void AsOfJoinInputPhysicalOperator::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

const Record::RecordFieldIdentifier AsOfJoinPhysicalOperator::KEY_HASH_FIELD
    = QualifiedIdentifier::create(Identifier::parse("asofjoin"), Identifier::parse("keyhash"));
const Record::RecordFieldIdentifier AsOfJoinPhysicalOperator::ROW_TIMESTAMP_FIELD
    = QualifiedIdentifier::create(Identifier::parse("asofjoin"), Identifier::parse("rowts"));
const Record::RecordFieldIdentifier AsOfJoinPhysicalOperator::ROW_NUMBER_FIELD
    = QualifiedIdentifier::create(Identifier::parse("asofjoin"), Identifier::parse("rownumber"));

AsOfJoinPhysicalOperator::AsOfJoinPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    AsOfJoinIndex index,
    PendingRowsLayout pendingRows,
    std::shared_ptr<TupleBufferRef> leftInputBufferRef,
    std::shared_ptr<TupleBufferRef> rightInputBufferRef,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    const OriginId outputOriginId,
    std::unique_ptr<TimeFunction> leftTimeFunction,
    std::unique_ptr<TimeFunction> rightTimeFunction)
    : operatorHandlerId(operatorHandlerId)
    , index(std::move(index))
    , leftInputBufferRef(std::move(leftInputBufferRef))
    , rightInputBufferRef(std::move(rightInputBufferRef))
    , leftTupleLayout(std::move(leftTupleLayout))
    , rightTupleLayout(std::move(rightTupleLayout))
    , leftInputFields(this->leftInputBufferRef->getAllFieldNames())
    , rightInputFields(this->rightInputBufferRef->getAllFieldNames())
    , leftFields(getOrderedFieldNames(this->leftTupleLayout->getSchema()))
    , rightFields(getOrderedFieldNames(this->rightTupleLayout->getSchema()))
    , outputOriginId(outputOriginId)
    , leftTimeFunction(std::move(leftTimeFunction))
    , rightTimeFunction(std::move(rightTimeFunction))
    , pendingRows(std::move(pendingRows))
{
    PRECONDITION(this->leftTimeFunction && this->rightTimeFunction, "ASOF join requires left and right time functions");
    PRECONDITION(
        this->index.leftKeyFunctions.size() == this->index.keyFields.size()
            && this->index.rightKeyFunctions.size() == this->index.keyFields.size(),
        "ASOF join needs one key function per side and key field");
}

AsOfJoinPhysicalOperator::AsOfJoinPhysicalOperator(const AsOfJoinPhysicalOperator& other)
    : PhysicalOperatorConcept(other.id)
    , operatorHandlerId(other.operatorHandlerId)
    , index(other.index)
    , leftInputBufferRef(other.leftInputBufferRef)
    , rightInputBufferRef(other.rightInputBufferRef)
    , leftTupleLayout(other.leftTupleLayout)
    , rightTupleLayout(other.rightTupleLayout)
    , leftInputFields(other.leftInputFields)
    , rightInputFields(other.rightInputFields)
    , leftFields(other.leftFields)
    , rightFields(other.rightFields)
    , outputOriginId(other.outputOriginId)
    , leftTimeFunction(other.leftTimeFunction->clone())
    , rightTimeFunction(other.rightTimeFunction->clone())
    , comparator(other.comparator)
    , pendingRows(other.pendingRows)
    , pendingComparator(other.pendingComparator)
    , child(other.child)
{
}

void AsOfJoinPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();

    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto isRight = invoke(
        +[](OperatorHandler* ptr, const OriginId originId)
        { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).isTableOrigin(originId); },
        handler,
        executionCtx.originId);

    if (isRight)
    {
        rightTimeFunction->open(executionCtx, recordBuffer);
    }
    else
    {
        leftTimeFunction->open(executionCtx, recordBuffer);
    }
    executionCtx.watermarkTs = nautilus::val<Timestamp>{invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getOutputWatermark().getRawValue(); },
        handler)};
    executionCtx.originId = outputOriginId;
    executionCtx.sequenceNumber = nautilus::val<SequenceNumber>{invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNextOutputSequence().getRawValue(); },
        handler)};
    executionCtx.chunkNumber = nautilus::val<ChunkNumber>{INITIAL_CHUNK_NUMBER};
    executionCtx.lastChunk = true;
    openChild(executionCtx, recordBuffer);

    const auto numberOfRecords = recordBuffer.getNumRecords();
    if (isRight)
    {
        invoke(lockHandler, handler);
        const auto rightState = getRightState(executionCtx);
        for (nautilus::val<uint64_t> recordIndex = 0; recordIndex < numberOfRecords; recordIndex = recordIndex + nautilus::val<uint64_t>{1})
        {
            auto record = rightInputBufferRef->readRecord(rightInputFields, recordBuffer, recordIndex);
            processRightRecord(executionCtx, record, rightState);
        }
        invoke(unlockHandler, handler);
    }
    else
    {
        /// A sealed right side no longer changes (no inserts, no compaction) and no left row is pending any more, so the buffer is
        /// probed without the lock. Otherwise the lock is held for the whole buffer.
        const auto sealed = invoke(isRightSealed, handler);
        nautilus::val<Timestamp> rightWatermark{Timestamp{Timestamp::INFINITE_VALUE}};
        if (!sealed)
        {
            invoke(lockHandler, handler);
            rightWatermark = nautilus::val<Timestamp>{invoke(getRightWatermark, handler)};
        }
        for (nautilus::val<uint64_t> recordIndex = 0; recordIndex < numberOfRecords; recordIndex = recordIndex + nautilus::val<uint64_t>{1})
        {
            auto record = leftInputBufferRef->readRecord(leftInputFields, recordBuffer, recordIndex);
            processLeftRecord(executionCtx, record, rightWatermark);
        }
        if (!sealed)
        {
            invoke(unlockHandler, handler);
        }
    }
}

nautilus::val<TupleBuffer*> AsOfJoinPhysicalOperator::getRightState(ExecutionContext& executionCtx) const
{
    return invoke(
        getRightTree, executionCtx.getGlobalOperatorHandler(operatorHandlerId), executionCtx.pipelineMemoryProvider.bufferProvider);
}

void AsOfJoinPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    /// Entries ordered by (key hash, timestamp, insertion number descending): rows with equal keys are adjacent and ordered by time, and
    /// among equal timestamps the first inserted row comes last, where a probe looks first.
    comparator = std::make_shared<BTreeComparator>(
        compilationContext,
        index.sortKeyLayout,
        fmt::format("asofJoinComparator:{}", id.getRawValue()),
        [](const Record& lhs, const Record& rhs) -> nautilus::val<bool>
        {
            const auto lhsHash = readUInt64(lhs, KEY_HASH_FIELD);
            const auto rhsHash = readUInt64(rhs, KEY_HASH_FIELD);
            const auto lhsTimestamp = readUInt64(lhs, ROW_TIMESTAMP_FIELD);
            const auto rhsTimestamp = readUInt64(rhs, ROW_TIMESTAMP_FIELD);
            const auto lhsNumber = readUInt64(lhs, ROW_NUMBER_FIELD);
            const auto rhsNumber = readUInt64(rhs, ROW_NUMBER_FIELD);
            return lhsHash < rhsHash
                || (lhsHash == rhsHash && (lhsTimestamp < rhsTimestamp || (lhsTimestamp == rhsTimestamp && lhsNumber > rhsNumber)));
        });
    pendingComparator = createPendingRowsComparator(compilationContext, pendingRows, id);
    PhysicalOperatorConcept::setup(executionCtx, compilationContext);
}

nautilus::val<bool>
AsOfJoinPhysicalOperator::writeKeys(ExecutionContext& executionCtx, Record& record, const std::vector<PhysicalFunction>& keyFunctions) const
{
    nautilus::val<bool> containsNull{false};
    std::vector<VarVal> keyValues;
    for (nautilus::static_val<uint64_t> i = 0; i < keyFunctions.size(); ++i)
    {
        const auto value = keyFunctions[i].execute(record, executionCtx.pipelineMemoryProvider.arena);
        containsNull = containsNull or (value.isNullable() and value.isNull());
        keyValues.push_back(value);
    }
    for (nautilus::static_val<uint64_t> i = 0; i < keyValues.size(); ++i)
    {
        record.write(index.keyFields[i].getFullyQualifiedName(), keyValues[i]);
    }
    /// Without equality predicate, every right row is a candidate: all rows share hash 0.
    const MurMur3HashFunction murmur3;
    const nautilus::val<uint64_t> hash
        = keyValues.empty() ? nautilus::val<uint64_t>{0} : static_cast<const HashFunction&>(murmur3).calculate(keyValues);
    record.write(KEY_HASH_FIELD, VarVal{hash});
    return containsNull;
}

nautilus::val<bool> AsOfJoinPhysicalOperator::keysEqual(const Record& lhs, const Record& rhs) const
{
    nautilus::val<bool> equal{true};
    for (const auto& keyField : nautilus::static_iterable(index.keyFields))
    {
        const auto& name = keyField.getFullyQualifiedName();
        if (keyField.getDataType().isType(DataType::Type::VARSIZED))
        {
            equal = equal and not(lhs.read(name).getRawValueAs<VariableSizedData>() != rhs.read(name).getRawValueAs<VariableSizedData>());
        }
        else
        {
            equal = equal and (lhs.read(name) == rhs.read(name)).getRawValueAs<nautilus::val<bool>>();
        }
    }
    return equal;
}

/// Expects the handler lock to be held.
void AsOfJoinPhysicalOperator::processRightRecord(
    ExecutionContext& executionCtx, Record& record, const nautilus::val<TupleBuffer*>& rightState) const
{
    /// A row with a null key never equals a left key, so it is not stored.
    if (writeKeys(executionCtx, record, index.rightKeyFunctions))
    {
        return;
    }
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto rowNumber
        = invoke(+[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).nextTableRowNumber(); }, handler);
    record.write(ROW_TIMESTAMP_FIELD, VarVal{rightTimeFunction->getTs(executionCtx, record).convertToValue()});
    record.write(ROW_NUMBER_FIELD, VarVal{rowNumber});
    const BTreeRef rightTree{BorrowedNautilusBuffer::from(rightState), index.entryLayout};
    rightTree.append(record, executionCtx.pipelineMemoryProvider.bufferProvider, *comparator);
}

/// Expects the handler lock to be held unless the right side is sealed.
void AsOfJoinPhysicalOperator::processLeftRecord(
    ExecutionContext& executionCtx, Record& record, const nautilus::val<Timestamp>& rightWatermark) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto leftTimestamp = leftTimeFunction->getTs(executionCtx, record);
    if (rightWatermark > leftTimestamp)
    {
        probeLeftRecord(executionCtx, record, leftTimestamp);
    }
    else
    {
        appendPendingRow(executionCtx, operatorHandlerId, pendingRows, *pendingComparator, record, leftTimestamp);
    }
}

void AsOfJoinPhysicalOperator::probeLeftRecord(
    ExecutionContext& executionCtx, const Record& leftRecord, const nautilus::val<Timestamp>& leftTimestamp) const
{
    /// The keys are written into a copy of the left record, next to the fields the key functions read.
    Record keys = leftRecord;
    if (writeKeys(executionCtx, keys, index.leftKeyFunctions))
    {
        return;
    }
    const auto hash = readUInt64(keys, KEY_HASH_FIELD);
    Record search;
    search.write(KEY_HASH_FIELD, VarVal{hash});
    search.write(ROW_TIMESTAMP_FIELD, VarVal{leftTimestamp.convertToValue()});
    search.write(ROW_NUMBER_FIELD, VarVal{nautilus::val<uint64_t>{0}});

    /// Probes of a sealed right side run without the lock, so they must not create the right state: an empty right side has none and
    /// matches nothing.
    const auto rightState = invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableIndexIfExists(); },
        executionCtx.getGlobalOperatorHandler(operatorHandlerId));
    if (rightState == nullptr)
    {
        return;
    }
    const BTreeRef rightTree{BorrowedNautilusBuffer::from(rightState), index.entryLayout};
    /// Every entry before the upper bound has a smaller hash, or this hash and a timestamp not after the left timestamp, so the entry just
    /// before it is the predecessor unless that entry belongs to another key with a colliding hash.
    nautilus::val<uint64_t> position{0};
    const auto lastAddress = rightTree.lastEntryNotAfter(search, *comparator, position);
    if (lastAddress == nullptr)
    {
        return;
    }
    const auto last = rightTree.readEntry(lastAddress);
    const auto lastHasHash = readUInt64(last, KEY_HASH_FIELD) == hash;
    if (lastHasHash && keysEqual(last, keys))
    {
        emitJoinedRecord(executionCtx, leftRecord, last);
        return;
    }
    if (!lastHasHash)
    {
        return;
    }
    /// Rare: step back over rows of other keys with the same hash. No `break`: the loop state carries the exit condition.
    position = position - nautilus::val<uint64_t>{1};
    nautilus::val<uint64_t> match{UINT64_MAX};
    nautilus::val<bool> searching{true};
    while (searching && position > nautilus::val<uint64_t>{0})
    {
        position = position - nautilus::val<uint64_t>{1};
        const auto candidate = rightTree.at(position);
        const auto sameHash = readUInt64(candidate, KEY_HASH_FIELD) == hash;
        const auto found = sameHash && keysEqual(candidate, keys);
        match = nautilus::select(found, position, match);
        searching = sameHash && !found;
    }
    if (match != nautilus::val<uint64_t>{UINT64_MAX})
    {
        emitJoinedRecord(executionCtx, leftRecord, rightTree.at(match));
    }
}

void AsOfJoinPhysicalOperator::emitJoinedRecord(ExecutionContext& executionCtx, const Record& leftRecord, const Record& rightRecord) const
{
    Record joinedRecord;
    for (const auto& field : nautilus::static_iterable(leftFields))
    {
        joinedRecord.write(field, leftRecord.read(field));
    }
    for (const auto& field : nautilus::static_iterable(rightFields))
    {
        joinedRecord.write(field, rightRecord.read(field));
    }
    executeChild(executionCtx, joinedRecord);
}

void AsOfJoinPhysicalOperator::releasePending(
    ExecutionContext& executionCtx, const nautilus::val<Timestamp>& rightWatermark, const nautilus::val<bool>& releaseAll) const
{
    releasePendingRows(
        executionCtx,
        operatorHandlerId,
        pendingRows,
        *pendingComparator,
        rightWatermark,
        releaseAll,
        [&](const Record& leftRecord, const nautilus::val<Timestamp>& timestamp) { probeLeftRecord(executionCtx, leftRecord, timestamp); });
}

/// Rebuilds the right index without the rows no future left row can select: per key, all rows not after `watermark` except the ASOF
/// predecessor at `watermark`. Left rows still to come have timestamps of at least `watermark`, so that predecessor is the oldest row any of
/// them can pick. Rows with later timestamps are kept. Rows with equal keys are adjacent and ordered by time, so a row not after the
/// watermark is obsolete if a following row of its hash run, also not after the watermark, has equal keys. Expects the handler lock to be held.
void AsOfJoinPhysicalOperator::compactRightState(ExecutionContext& executionCtx, const nautilus::val<Timestamp>& watermark) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto shouldCompact = invoke(
        +[](OperatorHandler* ptr, const Timestamp currentWatermark)
        { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).shouldCompactTableIndex(currentWatermark); },
        handler,
        watermark);
    if (!shouldCompact)
    {
        return;
    }
    const auto bufferProvider = executionCtx.pipelineMemoryProvider.bufferProvider;
    const BTreeRef oldTree{BorrowedNautilusBuffer::from(getRightState(executionCtx)), index.entryLayout};
    const BTreeRef newTree{
        BorrowedNautilusBuffer::from(invoke(
            +[](OperatorHandler* ptr, AbstractBufferProvider* provider)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).beginTableIndexCompaction(provider); },
            handler,
            bufferProvider)),
        index.entryLayout};
    const auto watermarkValue = watermark.convertToValue();
    const auto numberOfRows = oldTree.size();
    nautilus::val<uint64_t> retainedRows{0};
    for (nautilus::val<uint64_t> position = 0; position < numberOfRows; position = position + nautilus::val<uint64_t>{1})
    {
        const auto row = oldTree.at(position);
        const auto hash = readUInt64(row, KEY_HASH_FIELD);
        nautilus::val<bool> superseded{false};
        nautilus::val<bool> scanning = readUInt64(row, ROW_TIMESTAMP_FIELD) <= watermarkValue;
        nautilus::val<uint64_t> next = position + nautilus::val<uint64_t>{1};
        while (scanning && next < numberOfRows)
        {
            const auto other = oldTree.at(next);
            const auto inRun = readUInt64(other, KEY_HASH_FIELD) == hash && readUInt64(other, ROW_TIMESTAMP_FIELD) <= watermarkValue;
            const auto sameKeys = inRun && keysEqual(other, row);
            superseded = superseded || sameKeys;
            scanning = inRun && !sameKeys;
            next = next + nautilus::val<uint64_t>{1};
        }
        if (!superseded)
        {
            newTree.append(row, bufferProvider, *comparator);
            retainedRows = retainedRows + nautilus::val<uint64_t>{1};
        }
    }
    invoke(
        +[](OperatorHandler* ptr, const Timestamp currentWatermark, const uint64_t retained)
        { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).finishTableIndexCompaction(currentWatermark, retained); },
        handler,
        watermark,
        retainedRows);
}

void AsOfJoinPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto isRight = invoke(
        +[](OperatorHandler* ptr, const OriginId originId)
        { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).isTableOrigin(originId); },
        handler,
        recordBuffer.getOriginId());
    invoke(lockHandler, handler);
    if (isRight)
    {
        /// The right state must exist before the watermark can seal it: sealed probes read it without the lock.
        static_cast<void>(getRightState(executionCtx));
        const auto previousRightWatermark = nautilus::val<Timestamp>{invoke(getRightWatermark, handler)};
        const auto rightWatermark = nautilus::val<Timestamp>{invoke(
            +[](OperatorHandler* ptr,
                const Timestamp watermark,
                const SequenceNumber sequenceNumber,
                const ChunkNumber chunkNumber,
                const bool lastChunk,
                const OriginId originId)
            {
                return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr)
                    .updateTableWatermark(watermark, SequenceData{sequenceNumber, chunkNumber, lastChunk}, originId)
                    .getRawValue();
            },
            handler,
            recordBuffer.getWatermarkTs(),
            recordBuffer.getSequenceNumber(),
            recordBuffer.getChunkNumber(),
            recordBuffer.isLastChunk(),
            recordBuffer.getOriginId())};
        if (rightWatermark > previousRightWatermark)
        {
            releasePending(executionCtx, rightWatermark, rightWatermark == nautilus::val<Timestamp>{Timestamp{Timestamp::INFINITE_VALUE}});
        }
    }
    executionCtx.watermarkTs = nautilus::val<Timestamp>{invoke(
        +[](OperatorHandler* ptr,
            const Timestamp watermark,
            const SequenceNumber sequenceNumber,
            const ChunkNumber chunkNumber,
            const bool lastChunk,
            const OriginId originId)
        {
            return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr)
                .updateOutputWatermark(watermark, SequenceData{sequenceNumber, chunkNumber, lastChunk}, originId)
                .getRawValue();
        },
        handler,
        recordBuffer.getWatermarkTs(),
        recordBuffer.getSequenceNumber(),
        recordBuffer.getChunkNumber(),
        recordBuffer.isLastChunk(),
        recordBuffer.getOriginId())};
    compactRightState(executionCtx, executionCtx.watermarkTs);
    invoke(unlockHandler, handler);
    closeChild(executionCtx, recordBuffer);
}

void AsOfJoinPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(lockHandler, handler);
    executionCtx.watermarkTs = nautilus::val<Timestamp>{Timestamp{Timestamp::INFINITE_VALUE}};
    executionCtx.originId = outputOriginId;
    executionCtx.currentTs = nautilus::val<Timestamp>{Timestamp{Timestamp::INITIAL_VALUE}};
    executionCtx.sequenceNumber = nautilus::val<SequenceNumber>{invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNextOutputSequence().getRawValue(); },
        handler)};
    executionCtx.chunkNumber = nautilus::val<ChunkNumber>{INITIAL_CHUNK_NUMBER};
    executionCtx.lastChunk = true;

    RecordBuffer eosBuffer{executionCtx.allocateBuffer()};
    openChild(executionCtx, eosBuffer);
    releasePending(executionCtx, nautilus::val<Timestamp>{Timestamp{Timestamp::INVALID_VALUE}}, nautilus::val<bool>{true});
    closeChild(executionCtx, eosBuffer);

    invoke(unlockHandler, handler);
    terminateChild(executionCtx);
}

std::optional<PhysicalOperator> AsOfJoinPhysicalOperator::getChild() const
{
    return child;
}

void AsOfJoinPhysicalOperator::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

}
