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

#include <StreamTableJoin/StreamTableJoinPhysicalOperator.hpp>

#include <DataTypes/UnboundSchema.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <function.hpp>
#include <select.hpp>
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

TupleBuffer* getTableBuffer(OperatorHandler* handler, AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
{
    return dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).getOrCreateTableBuffer(bufferProvider, tupleSize);
}

TupleBuffer* getTableIndex(OperatorHandler* handler, AbstractBufferProvider* bufferProvider)
{
    return dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).getOrCreateTableIndex(bufferProvider);
}

bool isTableSealed(OperatorHandler* handler)
{
    return dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).isTableSealed();
}

}

void StreamTableJoinInputPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    executeChild(executionCtx, record);
}

void StreamTableJoinInputPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    closeChild(executionCtx, recordBuffer);
}

void StreamTableJoinInputPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    terminateChild(executionCtx);
}

std::optional<PhysicalOperator> StreamTableJoinInputPhysicalOperator::getChild() const
{
    return child;
}

void StreamTableJoinInputPhysicalOperator::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

StreamTableJoinPhysicalOperator::StreamTableJoinPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::optional<PhysicalFunction> joinFunction,
    std::optional<StreamTableJoinIndex> index,
    PendingRowsLayout pendingRows,
    std::shared_ptr<TupleBufferRef> streamInputBufferRef,
    std::shared_ptr<TupleBufferRef> tableInputBufferRef,
    std::shared_ptr<PagedVectorTupleLayout> streamTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> tableTupleLayout,
    const OriginId outputOriginId,
    std::unique_ptr<TimeFunction> streamTimeFunction,
    std::unique_ptr<TimeFunction> tableTimeFunction)
    : operatorHandlerId(operatorHandlerId)
    , joinFunction(std::move(joinFunction))
    , index(std::move(index))
    , streamInputBufferRef(std::move(streamInputBufferRef))
    , tableInputBufferRef(std::move(tableInputBufferRef))
    , streamTupleLayout(std::move(streamTupleLayout))
    , tableTupleLayout(std::move(tableTupleLayout))
    , streamInputFields(this->streamInputBufferRef->getAllFieldNames())
    , tableInputFields(this->tableInputBufferRef->getAllFieldNames())
    , streamFields(getOrderedFieldNames(this->streamTupleLayout->getSchema()))
    , tableFields(getOrderedFieldNames(this->tableTupleLayout->getSchema()))
    , outputOriginId(outputOriginId)
    , streamTimeFunction(std::move(streamTimeFunction))
    , tableTimeFunction(std::move(tableTimeFunction))
    , pendingRows(std::move(pendingRows))
{
}

StreamTableJoinPhysicalOperator::StreamTableJoinPhysicalOperator(const StreamTableJoinPhysicalOperator& other)
    : PhysicalOperatorConcept(other.id)
    , operatorHandlerId(other.operatorHandlerId)
    , joinFunction(other.joinFunction)
    , index(other.index)
    , streamInputBufferRef(other.streamInputBufferRef)
    , tableInputBufferRef(other.tableInputBufferRef)
    , streamTupleLayout(other.streamTupleLayout)
    , tableTupleLayout(other.tableTupleLayout)
    , streamInputFields(other.streamInputFields)
    , tableInputFields(other.tableInputFields)
    , streamFields(other.streamFields)
    , tableFields(other.tableFields)
    , outputOriginId(other.outputOriginId)
    , streamTimeFunction(other.streamTimeFunction ? other.streamTimeFunction->clone() : nullptr)
    , tableTimeFunction(other.tableTimeFunction ? other.tableTimeFunction->clone() : nullptr)
    , pendingRows(other.pendingRows)
    , pendingComparator(other.pendingComparator)
    , child(other.child)
{
}

void StreamTableJoinPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();

    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto isTable = invoke(
        +[](OperatorHandler* ptr, const OriginId originId)
        { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).isTableOrigin(originId); },
        handler,
        executionCtx.originId);

    if (isTable)
    {
        if (tableTimeFunction)
        {
            tableTimeFunction->open(executionCtx, recordBuffer);
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
        invoke(lockHandler, handler);
        const auto tableState = getTableState(executionCtx);
        const auto numberOfRecords = recordBuffer.getNumRecords();
        for (nautilus::val<uint64_t> recordIndex = 0; recordIndex < numberOfRecords; recordIndex = recordIndex + nautilus::val<uint64_t>{1})
        {
            auto record = tableInputBufferRef->readRecord(tableInputFields, recordBuffer, recordIndex);
            processTableRecord(executionCtx, record, tableState);
        }
        invoke(unlockHandler, handler);
    }
    else
    {
        if (streamTimeFunction)
        {
            streamTimeFunction->open(executionCtx, recordBuffer);
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
        /// A sealed table no longer changes and no stream row is pending any more, so the buffer is probed without the lock.
        /// Otherwise the lock is held for the whole buffer, which keeps the table watermark stable while its rows are processed.
        const auto sealed = invoke(isTableSealed, handler);
        nautilus::val<Timestamp> tableWatermark{Timestamp{Timestamp::INFINITE_VALUE}};
        if (!sealed)
        {
            invoke(lockHandler, handler);
            tableWatermark = nautilus::val<Timestamp>{invoke(
                +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableWatermark().getRawValue(); },
                handler)};
        }
        const auto numberOfRecords = recordBuffer.getNumRecords();
        for (nautilus::val<uint64_t> recordIndex = 0; recordIndex < numberOfRecords; recordIndex = recordIndex + nautilus::val<uint64_t>{1})
        {
            auto record = streamInputBufferRef->readRecord(streamInputFields, recordBuffer, recordIndex);
            processStreamRecord(executionCtx, record, tableWatermark);
        }
        if (!sealed)
        {
            invoke(unlockHandler, handler);
        }
    }
}

nautilus::val<TupleBuffer*> StreamTableJoinPhysicalOperator::getTableState(ExecutionContext& executionCtx) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    if (index)
    {
        return invoke(getTableIndex, handler, executionCtx.pipelineMemoryProvider.bufferProvider);
    }
    return invoke(
        getTableBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{getSizeInBytes(tableTupleLayout->getSchema())});
}

std::pair<Record, nautilus::val<bool>> StreamTableJoinPhysicalOperator::computeKeys(
    ExecutionContext& executionCtx, const Record& record, const std::vector<PhysicalFunction>& keyFunctions) const
{
    Record keys;
    nautilus::val<bool> containsNull{false};
    for (nautilus::static_val<uint64_t> i = 0; i < keyFunctions.size(); ++i)
    {
        const auto& keyField = index->hashMapConfig.fieldKeys[i].fieldIdentifier;
        const auto value = keyFunctions[i].execute(record, executionCtx.pipelineMemoryProvider.arena);
        containsNull = containsNull or (value.isNullable() and value.isNull());
        keys.write(keyField, value);
    }
    return {keys, containsNull};
}

/// Expects the handler lock to be held.
void StreamTableJoinPhysicalOperator::processTableRecord(
    ExecutionContext& executionCtx, Record& record, const nautilus::val<TupleBuffer*>& tableState) const
{
    const auto bufferProvider = executionCtx.pipelineMemoryProvider.bufferProvider;
    if (index)
    {
        /// A row with a null key never satisfies the equi-join keys of an inner join, so it is not stored.
        const auto [keys, containsNull] = computeKeys(executionCtx, record, index->tableKeyFunctions);
        if (not containsNull)
        {
            const auto tableIndexBuffer = BorrowedNautilusBuffer::from(tableState);
            ChainedHashMapRef tableIndex{tableIndexBuffer, index->hashMapConfig};
            const auto entry = tableIndex.appendEntry(keys, bufferProvider);
            const ChainedHashMapRef::ChainedEntryRef entryRef{
                entry, tableIndexBuffer, index->hashMapConfig.fieldKeys, index->hashMapConfig.fieldValues};
            entryRef.copyValuesToEntry(record, bufferProvider);
        }
    }
    else
    {
        PagedVectorRef tableRows{BorrowedNautilusBuffer::from(tableState), tableTupleLayout};
        tableRows.pushBack(record, bufferProvider);
    }
}

/// Expects the handler lock to be held unless the table is sealed.
void StreamTableJoinPhysicalOperator::processStreamRecord(
    ExecutionContext& executionCtx, Record& record, const nautilus::val<Timestamp>& tableWatermark) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const nautilus::val<Timestamp> streamTimestamp = streamTimeFunction ? streamTimeFunction->getTs(executionCtx, record)
                                                                        : nautilus::val<Timestamp>{Timestamp{Timestamp::INVALID_VALUE}};
    /// Without a time field, only the complete table (watermark infinity) is probed, so no stream row sees a partially loaded table.
    nautilus::val<bool> probeNow = tableWatermark == nautilus::val<Timestamp>{Timestamp{Timestamp::INFINITE_VALUE}};
    if (streamTimeFunction)
    {
        probeNow = probeNow or tableWatermark > streamTimestamp;
    }
    if (probeNow)
    {
        probeStreamRecord(executionCtx, record, streamTimestamp);
    }
    else
    {
        appendPendingRow(executionCtx, operatorHandlerId, pendingRows, *pendingComparator, record, streamTimestamp);
    }
}

void StreamTableJoinPhysicalOperator::probeStreamRecord(
    ExecutionContext& executionCtx, const Record& streamRecord, const nautilus::val<Timestamp>& streamTimestamp) const
{
    /// Probes of a sealed table run without the lock, so they must not create the table state: an empty table has none and matches nothing.
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto tableState = index
        ? invoke(
              +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableIndexIfExists(); }, handler)
        : invoke(
              +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableBufferIfExists(); }, handler);
    if (tableState == nullptr)
    {
        return;
    }
    if (index)
    {
        /// A null key equals nothing in SQL, but the hash map groups nulls together, so null keys are not looked up.
        const auto [keys, containsNull] = computeKeys(executionCtx, streamRecord, index->streamKeyFunctions);
        if (not containsNull)
        {
            const ChainedHashMapRef tableIndex{BorrowedNautilusBuffer::from(tableState), index->hashMapConfig};
            tableIndex.forEachMatch(
                keys,
                [&](const ChainedHashMapRef::ChainedEntryRef& entryRef)
                {
                    auto tableRecord = entryRef.getValue();
                    emitJoinedRecord(executionCtx, streamRecord, tableRecord, streamTimestamp);
                });
        }
    }
    else
    {
        /// The iterator keeps the current page, unlike at(), which searches for the page of every row.
        const PagedVectorRef tableRows{BorrowedNautilusBuffer::from(tableState), tableTupleLayout};
        for (auto it = tableRows.begin(); it != tableRows.end(); ++it)
        {
            auto tableRecord = *it;
            emitJoinedRecord(executionCtx, streamRecord, tableRecord, streamTimestamp);
        }
    }
}

void StreamTableJoinPhysicalOperator::emitJoinedRecord(
    ExecutionContext& executionCtx, const Record& streamRecord, Record& tableRecord, const nautilus::val<Timestamp>& streamTimestamp) const
{
    if (tableTimeFunction && tableTimeFunction->getTs(executionCtx, tableRecord) > streamTimestamp)
    {
        return;
    }

    Record joinedRecord;
    for (const auto& field : nautilus::static_iterable(streamFields))
    {
        joinedRecord.write(field, streamRecord.read(field));
    }
    for (const auto& field : nautilus::static_iterable(tableFields))
    {
        joinedRecord.write(field, tableRecord.read(field));
    }

    if (joinFunction)
    {
        if (joinFunction->execute(joinedRecord, executionCtx.pipelineMemoryProvider.arena))
        {
            executeChild(executionCtx, joinedRecord);
        }
    }
    else
    {
        executeChild(executionCtx, joinedRecord);
    }
}

const Record::RecordFieldIdentifier& pendingTimestampField()
{
    static const Record::RecordFieldIdentifier field
        = QualifiedIdentifier::create(Identifier::parse("streamtablejoin"), Identifier::parse("pendingts"));
    return field;
}

std::shared_ptr<BTreeComparator>
createPendingRowsComparator(CompilationContext& compilationContext, const PendingRowsLayout& layout, const OperatorId operatorId)
{
    return std::make_shared<BTreeComparator>(
        compilationContext,
        layout.sortKeyLayout,
        fmt::format("streamTableJoinPendingComparator:{}", operatorId.getRawValue()),
        [](const Record& lhs, const Record& rhs) -> nautilus::val<bool>
        {
            return lhs.read(pendingTimestampField()).getRawValueAs<nautilus::val<uint64_t>>()
                < rhs.read(pendingTimestampField()).getRawValueAs<nautilus::val<uint64_t>>();
        });
}

namespace
{
nautilus::val<TupleBuffer*> getPendingTree(ExecutionContext& executionCtx, const OperatorHandlerId operatorHandlerId)
{
    return invoke(
        +[](OperatorHandler* ptr, AbstractBufferProvider* bufferProvider)
        { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getOrCreatePendingTree(bufferProvider); },
        executionCtx.getGlobalOperatorHandler(operatorHandlerId),
        executionCtx.pipelineMemoryProvider.bufferProvider);
}
}

void appendPendingRow(
    ExecutionContext& executionCtx,
    const OperatorHandlerId operatorHandlerId,
    const PendingRowsLayout& layout,
    const BTreeComparator& comparator,
    Record& row,
    const nautilus::val<Timestamp>& timestamp)
{
    row.write(pendingTimestampField(), VarVal{timestamp.convertToValue()});
    const BTreeRef pendingTree{BorrowedNautilusBuffer::from(getPendingTree(executionCtx, operatorHandlerId)), layout.entryLayout};
    pendingTree.append(row, executionCtx.pipelineMemoryProvider.bufferProvider, comparator);
}

void releasePendingRows(
    ExecutionContext& executionCtx,
    const OperatorHandlerId operatorHandlerId,
    const PendingRowsLayout& layout,
    const BTreeComparator& comparator,
    const nautilus::val<Timestamp>& watermark,
    const nautilus::val<bool>& releaseAll,
    const std::function<void(const Record&, const nautilus::val<Timestamp>&)>& probe)
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto bufferProvider = executionCtx.pipelineMemoryProvider.bufferProvider;
    const BTreeRef pendingTree{BorrowedNautilusBuffer::from(getPendingTree(executionCtx, operatorHandlerId)), layout.entryLayout};
    const auto numberOfRows = pendingTree.size();
    const auto firstWaiting = invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getReleasedPendingRows(); }, handler);

    /// The rows before the watermark: released rows and the rows to release now, all in front of the rows that keep waiting.
    Record watermarkRow;
    watermarkRow.write(pendingTimestampField(), VarVal{watermark.convertToValue()});
    const auto endOfReleased = nautilus::select(releaseAll, numberOfRows, pendingTree.lowerBound(watermarkRow, comparator));

    for (auto it = pendingTree.iteratorAt(firstWaiting); it != pendingTree.end(endOfReleased); ++it)
    {
        const auto row = *it;
        probe(row, nautilus::val<Timestamp>{row.read(pendingTimestampField()).getRawValueAs<nautilus::val<uint64_t>>()});
    }

    const auto rebuild = invoke(
        +[](OperatorHandler* ptr, const uint64_t releasedRows)
        { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).setReleasedPendingRows(releasedRows); },
        handler,
        endOfReleased);
    if (rebuild)
    {
        /// Released rows make up half of the tree: copy the waiting rows into a new tree, which costs at most as much as releasing did.
        const BTreeRef compactedTree{
            BorrowedNautilusBuffer::from(invoke(
                +[](OperatorHandler* ptr, AbstractBufferProvider* provider)
                { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).beginPendingCompaction(provider); },
                handler,
                bufferProvider)),
            layout.entryLayout};
        for (auto it = pendingTree.iteratorAt(endOfReleased); it != pendingTree.end(numberOfRows); ++it)
        {
            compactedTree.append(*it, bufferProvider, comparator);
        }
        invoke(+[](OperatorHandler* ptr) { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).finishPendingCompaction(); }, handler);
    }
}

void StreamTableJoinPhysicalOperator::releasePending(
    ExecutionContext& executionCtx, const nautilus::val<Timestamp>& tableWatermark, const nautilus::val<bool>& releaseAll) const
{
    releasePendingRows(
        executionCtx,
        operatorHandlerId,
        pendingRows,
        *pendingComparator,
        tableWatermark,
        releaseAll,
        [&](const Record& streamRecord, const nautilus::val<Timestamp>& timestamp)
        { probeStreamRecord(executionCtx, streamRecord, timestamp); });
}

void StreamTableJoinPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto isTable = invoke(
        +[](OperatorHandler* ptr, const OriginId originId)
        { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).isTableOrigin(originId); },
        handler,
        recordBuffer.getOriginId());

    if (isTable)
    {
        invoke(lockHandler, handler);
        /// The table state must exist before the watermark can seal the table: sealed probes read it without the lock.
        static_cast<void>(getTableState(executionCtx));
        const auto previousTableWatermark = nautilus::val<Timestamp>{invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableWatermark().getRawValue(); },
            handler)};
        const auto tableWatermark = nautilus::val<Timestamp>{invoke(
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
        /// Without a time field, the table watermark only moves once: from zero to infinity when the table is complete.
        if (tableWatermark > previousTableWatermark)
        {
            releasePending(executionCtx, tableWatermark, tableWatermark == nautilus::val<Timestamp>{Timestamp{Timestamp::INFINITE_VALUE}});
        }
    }
    else
    {
        invoke(lockHandler, handler);
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
    }
    invoke(unlockHandler, handler);
    closeChild(executionCtx, recordBuffer);
}

void StreamTableJoinPhysicalOperator::terminate(ExecutionContext& executionCtx) const
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

void StreamTableJoinPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    pendingComparator = createPendingRowsComparator(compilationContext, pendingRows, id);
    PhysicalOperatorConcept::setup(executionCtx, compilationContext);
}

std::optional<PhysicalOperator> StreamTableJoinPhysicalOperator::getChild() const
{
    return child;
}

void StreamTableJoinPhysicalOperator::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

}
