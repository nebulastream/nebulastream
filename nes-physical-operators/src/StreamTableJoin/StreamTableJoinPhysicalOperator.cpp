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

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include <DataTypes/VarVal.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Common.hpp>
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

TupleBuffer* beginPendingCompaction(OperatorHandler* handler, AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
{
    return dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).beginPendingCompaction(bufferProvider, tupleSize);
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
    const JoinType joinType,
    PhysicalFunction joinFunction,
    std::shared_ptr<TupleBufferRef> streamInputBufferRef,
    std::shared_ptr<TupleBufferRef> tableInputBufferRef,
    std::shared_ptr<PagedVectorTupleLayout> streamTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> tableTupleLayout,
    const OriginId outputOriginId,
    std::optional<Record::RecordFieldIdentifier> markField,
    std::unique_ptr<TimeFunction> streamTimeFunction,
    std::unique_ptr<TimeFunction> tableTimeFunction)
    : operatorHandlerId(operatorHandlerId)
    , joinType(joinType)
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
    , markField(std::move(markField))
    , streamTimeFunction(std::move(streamTimeFunction))
    , tableTimeFunction(std::move(tableTimeFunction))
{
    PRECONDITION(this->markField.has_value() == (joinType == JoinType::MARK_APPLY), "Only mark apply requires a mark field");
}

StreamTableJoinPhysicalOperator::StreamTableJoinPhysicalOperator(const StreamTableJoinPhysicalOperator& other)
    : PhysicalOperatorConcept(other.id)
    , operatorHandlerId(other.operatorHandlerId)
    , joinType(other.joinType)
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
    , markField(other.markField)
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
        executionCtx.watermarkTs = invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getOutputWatermark(); }, handler);
        executionCtx.originId = outputOriginId;
        executionCtx.sequenceNumber = invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNextOutputSequence(); }, handler);
        executionCtx.chunkNumber = nautilus::val<ChunkNumber>{INITIAL_CHUNK_NUMBER};
        executionCtx.lastChunk = true;
        openChild(executionCtx, recordBuffer);
        const auto numberOfRecords = recordBuffer.getNumRecords();
        for (nautilus::val<uint64_t> index = 0; index < numberOfRecords; index = index + nautilus::val<uint64_t>{1})
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
        executionCtx.watermarkTs = invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getOutputWatermark(); }, handler);
        executionCtx.originId = outputOriginId;
        executionCtx.sequenceNumber = invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNextOutputSequence(); }, handler);
        executionCtx.chunkNumber = nautilus::val<ChunkNumber>{INITIAL_CHUNK_NUMBER};
        executionCtx.lastChunk = true;
        openChild(executionCtx, recordBuffer);
        const auto numberOfRecords = recordBuffer.getNumRecords();
        for (nautilus::val<uint64_t> index = 0; index < numberOfRecords; index = index + nautilus::val<uint64_t>{1})
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
    invoke(unlockHandler, handler);
}

void StreamTableJoinPhysicalOperator::processStreamRecord(ExecutionContext& executionCtx, Record& record) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(lockHandler, handler);

    const nautilus::val<Timestamp> streamTimestamp = streamTimeFunction ? streamTimeFunction->getTs(executionCtx, record)
                                                                        : nautilus::val<Timestamp>{Timestamp{Timestamp::INVALID_VALUE}};
    const auto tableWatermark
        = invoke(+[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableWatermark(); }, handler);
    if (streamTimeFunction && tableWatermark > streamTimestamp)
    {
        probeStreamRecord(executionCtx, record, streamTimestamp);
    }
    else
    {
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
    }
    invoke(unlockHandler, handler);
}

void StreamTableJoinPhysicalOperator::probeStreamRecord(
    ExecutionContext& executionCtx, const Record& streamRecord, const nautilus::val<Timestamp>& streamTimestamp) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto buffer = invoke(
        getTableBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{tableTupleLayout->getSchema().getSizeInBytes()});
    const PagedVectorRef tableState{BorrowedNautilusBuffer::from(buffer), tableTupleLayout};

    const auto numberOfTableRows = tableState.getNumberOfRecords();
    nautilus::val<bool> matched = false;
    nautilus::val<bool> sawNull = false;
    for (nautilus::val<uint64_t> tableIndex = 0; tableIndex < numberOfTableRows; tableIndex = tableIndex + nautilus::val<uint64_t>{1})
    {
        auto tableRecord = tableState.at(tableIndex);
        if (joinType == JoinType::INNER_JOIN || !matched)
        {
            emitJoinedRecord(executionCtx, streamRecord, tableRecord, streamTimestamp, matched, sawNull);
        }
    }

    if (joinType == JoinType::MARK_APPLY)
    {
        PRECONDITION(markField.has_value(), "Mark join is missing its output field");
        Record outputRecord;
        for (const auto& field : nautilus::static_iterable(streamFields))
        {
            outputRecord.write(field, streamRecord.read(field));
        }
        outputRecord.write(markField.value(), VarVal{matched, true, !matched && sawNull});
        executeChild(executionCtx, outputRecord);
    }
}

void StreamTableJoinPhysicalOperator::emitJoinedRecord(
    ExecutionContext& executionCtx,
    const Record& streamRecord,
    Record& tableRecord,
    const nautilus::val<Timestamp>& streamTimestamp,
    nautilus::val<bool>& matched,
    nautilus::val<bool>& sawNull) const
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

    const auto joinResult = joinFunction.execute(joinedRecord, executionCtx.pipelineMemoryProvider.arena);
    if (joinType == JoinType::MARK_APPLY)
    {
        sawNull = sawNull || joinResult.isNull();
        matched = matched || (!joinResult.isNull() && joinResult.getRawValueAs<nautilus::val<bool>>());
    }
    else if (joinResult)
    {
        if (joinType == JoinType::LEFT_SEMI_JOIN)
        {
            Record streamOutputRecord;
            for (const auto& field : nautilus::static_iterable(streamFields))
            {
                streamOutputRecord.write(field, streamRecord.read(field));
            }
            executeChild(executionCtx, streamOutputRecord);
            matched = true;
        }
        else
        {
            executeChild(executionCtx, joinedRecord);
        }
    }
}

void StreamTableJoinPhysicalOperator::releasePending(
    ExecutionContext& executionCtx, const nautilus::val<Timestamp>& tableWatermark, const nautilus::val<bool>& releaseAll) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto buffer = invoke(
        getPendingBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{streamTupleLayout->getSchema().getSizeInBytes()});
    const PagedVectorRef pendingState{BorrowedNautilusBuffer::from(buffer), streamTupleLayout};
    const auto numberOfPending = invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNumberOfPendingRows(); }, handler);
    const auto compactedBuffer = invoke(
        beginPendingCompaction,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{streamTupleLayout->getSchema().getSizeInBytes()});
    PagedVectorRef compactedPendingState{BorrowedNautilusBuffer::from(compactedBuffer), streamTupleLayout};

    for (nautilus::val<uint64_t> index = 0; index < numberOfPending; ++index)
    {
        const auto timestampRaw = invoke(
            +[](OperatorHandler* ptr, const uint64_t pos)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getPendingTimestamp(pos); },
            handler,
            index);
        const nautilus::val<Timestamp> timestamp{timestampRaw};
        const auto streamRecord = pendingState.at(index);
        if (releaseAll || tableWatermark > timestamp)
        {
            probeStreamRecord(executionCtx, streamRecord, timestamp);
        }
        else
        {
            compactedPendingState.pushBack(streamRecord, executionCtx.pipelineMemoryProvider.bufferProvider);
            invoke(
                +[](OperatorHandler* ptr, const uint64_t pendingTimestamp)
                { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).appendCompactedPendingTimestamp(pendingTimestamp); },
                handler,
                timestampRaw);
        }
    }
    invoke(+[](OperatorHandler* ptr) { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).finishPendingCompaction(); }, handler);
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
        const auto previousTableWatermark = invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableWatermark(); }, handler);
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
        if (tableTimeFunction && tableWatermark > previousTableWatermark)
        {
            releasePending(executionCtx, tableWatermark, nautilus::val<bool>{false});
        }
    }
    else
    {
        invoke(lockHandler, handler);
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
    }
    invoke(unlockHandler, handler);
    closeChild(executionCtx, recordBuffer);
}

void StreamTableJoinPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(lockHandler, handler);
    executionCtx.watermarkTs = nautilus::val<Timestamp>{Timestamp{Timestamp::INVALID_VALUE}};
    executionCtx.originId = outputOriginId;
    executionCtx.currentTs = nautilus::val<Timestamp>{Timestamp{Timestamp::INITIAL_VALUE}};
    executionCtx.sequenceNumber = invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNextOutputSequence(); }, handler);
    executionCtx.chunkNumber = nautilus::val<ChunkNumber>{INITIAL_CHUNK_NUMBER};
    executionCtx.lastChunk = true;

    RecordBuffer eosBuffer{executionCtx.allocateBuffer()};
    openChild(executionCtx, eosBuffer);
    releasePending(executionCtx, nautilus::val<Timestamp>{Timestamp{Timestamp::INVALID_VALUE}}, nautilus::val<bool>{true});
    closeChild(executionCtx, eosBuffer);

    invoke(unlockHandler, handler);
    terminateChild(executionCtx);
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
