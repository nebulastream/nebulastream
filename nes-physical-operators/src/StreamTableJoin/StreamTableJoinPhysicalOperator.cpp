/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <StreamTableJoin/StreamTableJoinPhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Common.hpp>
#include <Util/StdInt.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>

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

TupleBuffer* getPendingBuffer(OperatorHandler* handler, AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
{
    return dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).getOrCreatePendingBuffer(bufferProvider, tupleSize);
}
}

StreamTableJoinInputPhysicalOperator::StreamTableJoinInputPhysicalOperator(
    const OperatorHandlerId operatorHandlerId, std::vector<OriginId> inputOriginIds)
    : operatorHandlerId(operatorHandlerId), inputOriginIds(std::move(inputOriginIds))
{
}

void StreamTableJoinInputPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    executeChild(executionCtx, record);
}

void StreamTableJoinInputPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(lockHandler, handler);
    invoke(
        +[](OperatorHandler* ptr, const OriginId originId, const SequenceNumber sequenceNumber)
        { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).observeInputSequence(originId, sequenceNumber); },
        handler,
        executionCtx.originId,
        executionCtx.sequenceNumber);
    invoke(unlockHandler, handler);
    closeChild(executionCtx, recordBuffer);
}

void StreamTableJoinInputPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    for (const auto originId : inputOriginIds)
    {
        invoke(lockHandler, handler);
        const auto eosSequenceNumber = invoke(
            +[](OperatorHandler* ptr, const OriginId currentOriginId)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNextInputSequence(currentOriginId); },
            handler,
            nautilus::val<OriginId>{originId});
        invoke(unlockHandler, handler);

        RecordBuffer eosBuffer{executionCtx.allocateBuffer()};
        eosBuffer.setNumRecords(0_u64);
        eosBuffer.setOriginId(originId);
        eosBuffer.setWatermarkTs(nautilus::val<Timestamp>{Timestamp{Timestamp::INVALID_VALUE}});
        eosBuffer.setSequenceNumber(eosSequenceNumber);
        eosBuffer.setChunkNumber(nautilus::val<ChunkNumber>{INITIAL_CHUNK_NUMBER});
        eosBuffer.setLastChunk(true);
        eosBuffer.setCreationTs(nautilus::val<Timestamp>{Timestamp{Timestamp::INITIAL_VALUE}});
        executionCtx.emitBuffer(eosBuffer);
    }
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
    PhysicalFunction joinFunction,
    std::shared_ptr<TupleBufferRef> streamInputBufferRef,
    std::shared_ptr<TupleBufferRef> tableInputBufferRef,
    std::shared_ptr<PagedVectorTupleLayout> streamTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> tableTupleLayout,
    const OriginId outputOriginId,
    std::unique_ptr<TimeFunction> streamTimeFunction,
    std::unique_ptr<TimeFunction> tableTimeFunction)
    : operatorHandlerId(operatorHandlerId)
    , joinFunction(std::move(joinFunction))
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
{
}

StreamTableJoinPhysicalOperator::StreamTableJoinPhysicalOperator(const StreamTableJoinPhysicalOperator& other)
    : PhysicalOperatorConcept(other.id)
    , operatorHandlerId(other.operatorHandlerId)
    , joinFunction(other.joinFunction)
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
        executionCtx.originId = outputOriginId;
        executionCtx.sequenceNumber = invoke(
            +[](OperatorHandler* ptr)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNextOutputSequence(); },
            handler);
        executionCtx.chunkNumber = nautilus::val<ChunkNumber>{INITIAL_CHUNK_NUMBER};
        executionCtx.lastChunk = true;
        openChild(executionCtx, recordBuffer);
        const auto numberOfRecords = recordBuffer.getNumRecords();
        for (nautilus::val<uint64_t> index = 0_u64; index < numberOfRecords; index = index + 1_u64)
        {
            auto record = tableInputBufferRef->readRecord(tableInputFields, recordBuffer, index);
            processTableRecord(executionCtx, record);
        }
    }
    else
    {
        if (streamTimeFunction)
        {
            streamTimeFunction->open(executionCtx, recordBuffer);
        }
        executionCtx.originId = outputOriginId;
        executionCtx.sequenceNumber = invoke(
            +[](OperatorHandler* ptr)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNextOutputSequence(); },
            handler);
        executionCtx.chunkNumber = nautilus::val<ChunkNumber>{INITIAL_CHUNK_NUMBER};
        executionCtx.lastChunk = true;
        openChild(executionCtx, recordBuffer);
        const auto numberOfRecords = recordBuffer.getNumRecords();
        for (nautilus::val<uint64_t> index = 0_u64; index < numberOfRecords; index = index + 1_u64)
        {
            auto record = streamInputBufferRef->readRecord(streamInputFields, recordBuffer, index);
            processStreamRecord(executionCtx, record);
        }
    }
}

void StreamTableJoinPhysicalOperator::processTableRecord(ExecutionContext& executionCtx, Record& record) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(lockHandler, handler);
    const auto buffer = invoke(
        getTableBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{tableTupleLayout->getSchema().getSizeInBytes()});
    PagedVectorRef tableState{BorrowedNautilusBuffer::from(buffer), tableTupleLayout};
    tableState.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);

    const auto pendingBuffer = invoke(
        getPendingBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{streamTupleLayout->getSchema().getSizeInBytes()});
    const PagedVectorRef pendingState{BorrowedNautilusBuffer::from(pendingBuffer), streamTupleLayout};
    const auto numberOfReleasedRows = invoke(
        +[](OperatorHandler* ptr)
        { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNumberOfReleasedPendingRows(); },
        handler);
    for (nautilus::val<uint64_t> releasedIndex = 0; releasedIndex < numberOfReleasedRows; releasedIndex = releasedIndex + 1_u64)
    {
        const auto pendingIndex = invoke(
            +[](OperatorHandler* ptr, const uint64_t index)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getReleasedPendingRowIndex(index); },
            handler,
            releasedIndex);
        const auto timestampRaw = invoke(
            +[](OperatorHandler* ptr, const uint64_t index)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getPendingTimestamp(index); },
            handler,
            pendingIndex);
        const auto streamRecord = pendingState.at(pendingIndex);
        emitJoinedRecord(executionCtx, streamRecord, record, nautilus::val<Timestamp>{timestampRaw});
    }
    invoke(unlockHandler, handler);
}

void StreamTableJoinPhysicalOperator::processStreamRecord(ExecutionContext& executionCtx, Record& record) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(lockHandler, handler);

    const nautilus::val<Timestamp> streamTimestamp = streamTimeFunction
        ? streamTimeFunction->getTs(executionCtx, record)
        : nautilus::val<Timestamp>{Timestamp{Timestamp::INVALID_VALUE}};
    const auto tableComplete = invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).isTableComplete(); }, handler);
    const auto tableWatermark = invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableWatermark(); }, handler);

    const auto buffer = invoke(
        getPendingBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{streamTupleLayout->getSchema().getSizeInBytes()});
    PagedVectorRef pendingState{BorrowedNautilusBuffer::from(buffer), streamTupleLayout};
    pendingState.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);
    invoke(
        +[](OperatorHandler* ptr, const uint64_t timestamp)
        { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).appendPendingTimestamp(timestamp); },
        handler,
        streamTimestamp.convertToValue());

    if (tableComplete || (streamTimeFunction && tableWatermark > streamTimestamp))
    {
        const auto pendingIndex = invoke(
            +[](OperatorHandler* ptr)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNumberOfPendingRows() - 1; },
            handler);
        invoke(
            +[](OperatorHandler* ptr, const uint64_t index)
            { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).markPendingReleased(index); },
            handler,
            pendingIndex);
        probeStreamRecord(executionCtx, record, streamTimestamp);
    }
    invoke(unlockHandler, handler);
}

void StreamTableJoinPhysicalOperator::probeStreamRecord(
    ExecutionContext& executionCtx,
    const Record& streamRecord,
    const nautilus::val<Timestamp>& streamTimestamp) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto buffer = invoke(
        getTableBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{tableTupleLayout->getSchema().getSizeInBytes()});
    const PagedVectorRef tableState{BorrowedNautilusBuffer::from(buffer), tableTupleLayout};

    const auto numberOfTableRows = tableState.getNumberOfRecords();
    for (nautilus::val<uint64_t> tableIndex = 0; tableIndex < numberOfTableRows; tableIndex = tableIndex + 1_u64)
    {
        auto tableRecord = tableState.at(tableIndex);
        emitJoinedRecord(executionCtx, streamRecord, tableRecord, streamTimestamp);
    }
}

void StreamTableJoinPhysicalOperator::emitJoinedRecord(
    ExecutionContext& executionCtx,
    const Record& streamRecord,
    Record& tableRecord,
    const nautilus::val<Timestamp>& streamTimestamp) const
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

    if (joinFunction.execute(joinedRecord, executionCtx.pipelineMemoryProvider.arena))
    {
        executeChild(executionCtx, joinedRecord);
    }
}

void StreamTableJoinPhysicalOperator::releasePending(
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& tableWatermark,
    const bool releaseAll) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto buffer = invoke(
        getPendingBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{streamTupleLayout->getSchema().getSizeInBytes()});
    const PagedVectorRef pendingState{BorrowedNautilusBuffer::from(buffer), streamTupleLayout};
    const auto numberOfPending = invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNumberOfPendingRows(); },
        handler);

    for (nautilus::val<uint64_t> index = 0; index < numberOfPending; ++index)
    {
        const auto released = invoke(
            +[](OperatorHandler* ptr, const uint64_t pos)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).pendingWasReleased(pos); },
            handler,
            index);
        const auto timestampRaw = invoke(
            +[](OperatorHandler* ptr, const uint64_t pos)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getPendingTimestamp(pos); },
            handler,
            index);
        const nautilus::val<Timestamp> timestamp{timestampRaw};
        if (!released && (releaseAll || tableWatermark > timestamp))
        {
            const auto streamRecord = pendingState.at(index);
            probeStreamRecord(executionCtx, streamRecord, timestamp);
            invoke(
                +[](OperatorHandler* ptr, const uint64_t pos)
                { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).markPendingReleased(pos); },
                handler,
                index);
        }
    }
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
        const auto tableWatermark = invoke(
            +[](OperatorHandler* ptr,
                const Timestamp watermark,
                const SequenceNumber sequenceNumber,
                const ChunkNumber chunkNumber,
                const bool lastChunk,
                const OriginId originId)
            {
                return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).updateTableWatermark(
                    watermark, SequenceData{sequenceNumber, chunkNumber, lastChunk}, originId);
            },
            handler,
            recordBuffer.getWatermarkTs(),
            recordBuffer.getSequenceNumber(),
            recordBuffer.getChunkNumber(),
            recordBuffer.isLastChunk(),
            recordBuffer.getOriginId());
        const auto tableComplete = invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).isTableComplete(); },
            handler);
        if (tableComplete || tableTimeFunction)
        {
            releasePending(executionCtx, tableWatermark, tableComplete);
        }
    }
    else
    {
        invoke(lockHandler, handler);
    }
    executionCtx.watermarkTs = invoke(
        +[](OperatorHandler* ptr,
            const Timestamp watermark,
            const SequenceNumber sequenceNumber,
            const ChunkNumber chunkNumber,
            const bool lastChunk,
            const OriginId originId)
        {
            return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).updateOutputWatermark(
                watermark, SequenceData{sequenceNumber, chunkNumber, lastChunk}, originId);
        },
        handler,
        recordBuffer.getWatermarkTs(),
        recordBuffer.getSequenceNumber(),
        recordBuffer.getChunkNumber(),
        recordBuffer.isLastChunk(),
        recordBuffer.getOriginId());
    invoke(unlockHandler, handler);
    closeChild(executionCtx, recordBuffer);
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
