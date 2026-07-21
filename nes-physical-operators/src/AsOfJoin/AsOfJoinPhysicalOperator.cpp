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

#include <cstdint>
#include <memory>
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
#include <nautilus/select.hpp>
#include <nautilus/val_ptr.hpp>
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

TupleBuffer* getRightBuffer(OperatorHandler* handler, AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
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

AsOfJoinInputPhysicalOperator::AsOfJoinInputPhysicalOperator(
    const OperatorHandlerId operatorHandlerId, std::vector<OriginId> inputOriginIds)
    : operatorHandlerId(operatorHandlerId), inputOriginIds(std::move(inputOriginIds))
{
}

void AsOfJoinInputPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    executeChild(executionCtx, record);
}

void AsOfJoinInputPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
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

void AsOfJoinInputPhysicalOperator::terminate(ExecutionContext& executionCtx) const
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

std::optional<PhysicalOperator> AsOfJoinInputPhysicalOperator::getChild() const
{
    return child;
}

void AsOfJoinInputPhysicalOperator::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

AsOfJoinPhysicalOperator::AsOfJoinPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    std::shared_ptr<TupleBufferRef> leftInputBufferRef,
    std::shared_ptr<TupleBufferRef> rightInputBufferRef,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    const OriginId outputOriginId,
    std::unique_ptr<TimeFunction> leftTimeFunction,
    std::unique_ptr<TimeFunction> rightTimeFunction)
    : operatorHandlerId(operatorHandlerId)
    , joinFunction(std::move(joinFunction))
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
{
    PRECONDITION(this->leftTimeFunction && this->rightTimeFunction, "ASOF join requires left and right time functions");
}

AsOfJoinPhysicalOperator::AsOfJoinPhysicalOperator(const AsOfJoinPhysicalOperator& other)
    : PhysicalOperatorConcept(other.id)
    , operatorHandlerId(other.operatorHandlerId)
    , joinFunction(other.joinFunction)
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
        executionCtx.watermarkTs = invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getOutputWatermark(); }, handler);
        executionCtx.originId = outputOriginId;
        executionCtx.sequenceNumber = invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNextOutputSequence(); }, handler);
        executionCtx.chunkNumber = nautilus::val<ChunkNumber>{INITIAL_CHUNK_NUMBER};
        executionCtx.lastChunk = true;
        openChild(executionCtx, recordBuffer);
        const auto numberOfRecords = recordBuffer.getNumRecords();
        for (nautilus::val<uint64_t> index = 0_u64; index < numberOfRecords; index = index + 1_u64)
        {
            auto record = rightInputBufferRef->readRecord(rightInputFields, recordBuffer, index);
            processRightRecord(executionCtx, record);
        }
    }
    else
    {
        leftTimeFunction->open(executionCtx, recordBuffer);
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
        invoke(unlockHandler, handler);
        executionCtx.originId = outputOriginId;
        executionCtx.sequenceNumber = invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNextOutputSequence(); }, handler);
        executionCtx.chunkNumber = nautilus::val<ChunkNumber>{INITIAL_CHUNK_NUMBER};
        executionCtx.lastChunk = true;
        openChild(executionCtx, recordBuffer);
        const auto numberOfRecords = recordBuffer.getNumRecords();
        for (nautilus::val<uint64_t> index = 0_u64; index < numberOfRecords; index = index + 1_u64)
        {
            auto record = leftInputBufferRef->readRecord(leftInputFields, recordBuffer, index);
            processLeftRecord(executionCtx, record);
        }
    }
}

void AsOfJoinPhysicalOperator::processRightRecord(ExecutionContext& executionCtx, Record& record) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(lockHandler, handler);
    const auto buffer = invoke(
        getRightBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{rightTupleLayout->getSchema().getSizeInBytes()});
    PagedVectorRef rightState{BorrowedNautilusBuffer::from(buffer), rightTupleLayout};
    rightState.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);
    invoke(unlockHandler, handler);
}

void AsOfJoinPhysicalOperator::processLeftRecord(ExecutionContext& executionCtx, Record& record) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(lockHandler, handler);

    const auto leftTimestamp = leftTimeFunction->getTs(executionCtx, record);
    const auto rightComplete
        = invoke(+[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).isTableComplete(); }, handler);
    const auto rightWatermark
        = invoke(+[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableWatermark(); }, handler);
    if (rightComplete || rightWatermark > leftTimestamp)
    {
        probeLeftRecord(executionCtx, record, leftTimestamp);
    }
    else
    {
        const auto buffer = invoke(
            getPendingBuffer,
            handler,
            executionCtx.pipelineMemoryProvider.bufferProvider,
            nautilus::val<uint64_t>{leftTupleLayout->getSchema().getSizeInBytes()});
        PagedVectorRef pendingState{BorrowedNautilusBuffer::from(buffer), leftTupleLayout};
        pendingState.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);
        invoke(
            +[](OperatorHandler* ptr, const uint64_t timestamp)
            { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).appendPendingTimestamp(timestamp); },
            handler,
            leftTimestamp.convertToValue());
    }
    invoke(unlockHandler, handler);
}

void AsOfJoinPhysicalOperator::probeLeftRecord(
    ExecutionContext& executionCtx, const Record& leftRecord, const nautilus::val<Timestamp>& leftTimestamp) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto buffer = invoke(
        getRightBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{rightTupleLayout->getSchema().getSizeInBytes()});
    const PagedVectorRef rightState{BorrowedNautilusBuffer::from(buffer), rightTupleLayout};
    const auto numberOfRightRows = rightState.getNumberOfRecords();
    if (numberOfRightRows == 0_u64)
    {
        return;
    }

    nautilus::val<uint64_t*> selectionState = static_cast<nautilus::val<uint64_t*>>(
        executionCtx.pipelineMemoryProvider.arena.allocateMemory(nautilus::val<size_t>{2 * sizeof(uint64_t)}));
    nautilus::val<uint64_t*> bestRightTimestamp = selectionState;
    nautilus::val<uint64_t*> bestRightIndex = selectionState + 1_u64;
    *bestRightTimestamp = 0_u64;
    *bestRightIndex = nautilus::val<uint64_t>{UINT64_MAX};
    const auto leftTimestampValue = leftTimestamp.convertToValue();

    // ponytail: linear predecessor scan; replace with a timestamp index when long-running ASOF state becomes a measured bottleneck.
    for (nautilus::val<uint64_t> rightIndex = 0; rightIndex < numberOfRightRows; rightIndex = rightIndex + 1_u64)
    {
        auto rightRecord = rightState.at(rightIndex);
        const auto rightTimestampValue = rightTimeFunction->getTs(executionCtx, rightRecord).convertToValue();
        Record joinedRecord;
        for (const auto& field : nautilus::static_iterable(leftFields))
        {
            joinedRecord.write(field, leftRecord.read(field));
        }
        for (const auto& field : nautilus::static_iterable(rightFields))
        {
            joinedRecord.write(field, rightRecord.read(field));
        }
        const auto joinResult = joinFunction.execute(joinedRecord, executionCtx.pipelineMemoryProvider.arena);
        const auto qualifies
            = rightTimestampValue <= leftTimestampValue && !joinResult.isNull() && joinResult.getRawValueAs<nautilus::val<bool>>();
        const nautilus::val<uint64_t> currentBestTimestamp = *bestRightTimestamp;
        const nautilus::val<uint64_t> currentBestIndex = *bestRightIndex;
        const auto betterMatch
            = qualifies && (currentBestIndex == nautilus::val<uint64_t>{UINT64_MAX} || rightTimestampValue > currentBestTimestamp);
        *bestRightTimestamp = nautilus::select(betterMatch, rightTimestampValue, currentBestTimestamp);
        *bestRightIndex = nautilus::select(betterMatch, rightIndex, currentBestIndex);
    }

    const nautilus::val<uint64_t> selectedRightIndex = *bestRightIndex;
    if (selectedRightIndex == nautilus::val<uint64_t>{UINT64_MAX})
    {
        return;
    }
    const auto bestRightRecord = rightState.at(selectedRightIndex);
    Record joinedRecord;
    for (const auto& field : nautilus::static_iterable(leftFields))
    {
        joinedRecord.write(field, leftRecord.read(field));
    }
    for (const auto& field : nautilus::static_iterable(rightFields))
    {
        joinedRecord.write(field, bestRightRecord.read(field));
    }
    executeChild(executionCtx, joinedRecord);
}

void AsOfJoinPhysicalOperator::releasePending(
    ExecutionContext& executionCtx, const nautilus::val<Timestamp>& rightWatermark, const nautilus::val<bool>& releaseAll) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto buffer = invoke(
        getPendingBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{leftTupleLayout->getSchema().getSizeInBytes()});
    const PagedVectorRef pendingState{BorrowedNautilusBuffer::from(buffer), leftTupleLayout};
    const auto numberOfPending = invoke(
        +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getNumberOfPendingRows(); }, handler);
    const auto compactedBuffer = invoke(
        beginPendingCompaction,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{leftTupleLayout->getSchema().getSizeInBytes()});
    PagedVectorRef compactedPendingState{BorrowedNautilusBuffer::from(compactedBuffer), leftTupleLayout};

    for (nautilus::val<uint64_t> index = 0; index < numberOfPending; ++index)
    {
        const auto timestampRaw = invoke(
            +[](OperatorHandler* ptr, const uint64_t pos)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getPendingTimestamp(pos); },
            handler,
            index);
        const nautilus::val<Timestamp> timestamp{timestampRaw};
        const auto leftRecord = pendingState.at(index);
        if (releaseAll || rightWatermark > timestamp)
        {
            probeLeftRecord(executionCtx, leftRecord, timestamp);
        }
        else
        {
            compactedPendingState.pushBack(leftRecord, executionCtx.pipelineMemoryProvider.bufferProvider);
            invoke(
                +[](OperatorHandler* ptr, const uint64_t pendingTimestamp)
                { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).appendCompactedPendingTimestamp(pendingTimestamp); },
                handler,
                timestampRaw);
        }
    }
    invoke(+[](OperatorHandler* ptr) { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).finishPendingCompaction(); }, handler);
}

void AsOfJoinPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto isRight = invoke(
        +[](OperatorHandler* ptr, const OriginId originId)
        { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).isTableOrigin(originId); },
        handler,
        recordBuffer.getOriginId());

    if (isRight)
    {
        invoke(lockHandler, handler);
        const auto rightWatermark = invoke(
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
        const auto rightComplete
            = invoke(+[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).isTableComplete(); }, handler);
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
        releasePending(executionCtx, rightWatermark, rightComplete);
    }
    else
    {
        invoke(lockHandler, handler);
    }
    invoke(unlockHandler, handler);
    closeChild(executionCtx, recordBuffer);
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
