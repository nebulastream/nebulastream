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

#include <cstddef>
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
#include <nautilus/select.hpp>
#include <nautilus/val_ptr.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include "DataTypes/Schema.hpp"
#include "ErrorHandling.hpp"
#include "Functions/PhysicalFunction.hpp"
#include "Identifiers/Identifiers.hpp"
#include "Interface/BufferRef/TupleBufferRef.hpp"
#include "Interface/NESStrongTypeRef.hpp"
#include "Interface/Record.hpp"
#include "PhysicalOperator.hpp"
#include "Watermark/TimeFunction.hpp"

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

TupleBuffer* beginRightCompaction(OperatorHandler* handler, AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
{
    return dynamic_cast<StreamTableJoinOperatorHandler&>(*handler).beginTableCompaction(bufferProvider, tupleSize);
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

template <bool PredicateFree>
AsOfJoinPhysicalOperator<PredicateFree>::AsOfJoinPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    std::shared_ptr<TupleBufferRef> leftInputBufferRef,
    std::shared_ptr<TupleBufferRef> rightInputBufferRef,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    const OriginId outputOriginId,
    std::unique_ptr<TimeFunction> leftTimeFunction,
    std::unique_ptr<TimeFunction> rightTimeFunction,
    std::optional<Record::RecordFieldIdentifier> rightKeyField)
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
    , rightKeyField(std::move(rightKeyField))
{
    PRECONDITION(this->leftTimeFunction && this->rightTimeFunction, "ASOF join requires left and right time functions");
    PRECONDITION(PredicateFree != this->rightKeyField.has_value(), "Equality ASOF requires exactly one right key field");
}

template <bool PredicateFree>
AsOfJoinPhysicalOperator<PredicateFree>::AsOfJoinPhysicalOperator(const AsOfJoinPhysicalOperator& other)
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
    , rightKeyField(other.rightKeyField)
    , child(other.child)
{
}

template <bool PredicateFree>
void AsOfJoinPhysicalOperator<PredicateFree>::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
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
        for (nautilus::val<uint64_t> index = 0; index < numberOfRecords; index = index + nautilus::val<uint64_t>{1})
        {
            auto record = rightInputBufferRef->readRecord(rightInputFields, recordBuffer, index);
            processRightRecord(executionCtx, record);
        }
    }
    else
    {
        leftTimeFunction->open(executionCtx, recordBuffer);
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
            auto record = leftInputBufferRef->readRecord(leftInputFields, recordBuffer, index);
            processLeftRecord(executionCtx, record);
        }
    }
}

template <bool PredicateFree>
void AsOfJoinPhysicalOperator<PredicateFree>::processRightRecord(ExecutionContext& executionCtx, Record& record) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(lockHandler, handler);
    const auto rightTimestamp = rightTimeFunction->getTs(executionCtx, record).convertToValue();
    const auto buffer = invoke(
        getRightBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{rightTupleLayout->getSchema().getSizeInBytes()});
    PagedVectorRef rightState{BorrowedNautilusBuffer::from(buffer), rightTupleLayout};
    rightState.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);
    invoke(
        +[](OperatorHandler* ptr, const uint64_t timestamp)
        { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).appendTableTimestamp(timestamp); },
        handler,
        rightTimestamp);
    invoke(unlockHandler, handler);
}

template <bool PredicateFree>
void AsOfJoinPhysicalOperator<PredicateFree>::processLeftRecord(ExecutionContext& executionCtx, Record& record) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(lockHandler, handler);

    const auto leftTimestamp = leftTimeFunction->getTs(executionCtx, record);
    const auto rightWatermark
        = invoke(+[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableWatermark(); }, handler);
    if (rightWatermark > leftTimestamp)
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

template <bool PredicateFree>
void AsOfJoinPhysicalOperator<PredicateFree>::probeLeftRecord(
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
    if (numberOfRightRows == nautilus::val<uint64_t>{0})
    {
        return;
    }

    invoke(+[](OperatorHandler* ptr) { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).prepareTableTimestampOrder(); }, handler);

    nautilus::val<uint64_t*> selectedRightIndexState = static_cast<nautilus::val<uint64_t*>>(
        executionCtx.pipelineMemoryProvider.arena.allocateMemory(nautilus::val<size_t>{sizeof(uint64_t)}));
    *selectedRightIndexState = nautilus::val<uint64_t>{UINT64_MAX};
    const auto leftTimestampValue = leftTimestamp.convertToValue();
    const auto startPosition = invoke(
        +[](OperatorHandler* ptr, const uint64_t timestamp)
        { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getDescendingTimestampStartPosition(timestamp); },
        handler,
        leftTimestampValue);

    /// Traverse newest-to-oldest so the first qualifying row is the ASOF predecessor. The handler's timestamp index makes this independent
    /// of concurrent right-input insertion order and preserves the original first-row tie-break for equal timestamps.
    for (nautilus::val<uint64_t> position = startPosition;
         position < numberOfRightRows && *selectedRightIndexState == nautilus::val<uint64_t>{UINT64_MAX};
         position = position + nautilus::val<uint64_t>{1})
    {
        const auto rightIndex = invoke(
            +[](OperatorHandler* ptr, const uint64_t currentPosition)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableIndexByDescendingTimestampPosition(currentPosition); },
            handler,
            position);
        auto rightRecord = rightState.at(rightIndex);
        const auto rightTimestampValue = invoke(
            +[](OperatorHandler* ptr, const uint64_t index)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableTimestamp(index); },
            handler,
            rightIndex);
        const auto qualifies = [&]
        {
            if constexpr (PredicateFree)
            {
                return rightTimestampValue <= leftTimestampValue;
            }
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
            return rightTimestampValue <= leftTimestampValue && !joinResult.isNull() && joinResult.getRawValueAs<nautilus::val<bool>>();
        }();
        const nautilus::val<uint64_t> currentSelectedRightIndex = *selectedRightIndexState;
        *selectedRightIndexState = nautilus::select(qualifies, rightIndex, currentSelectedRightIndex);
    }

    const nautilus::val<uint64_t> selectedRightIndex = *selectedRightIndexState;
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

template <bool PredicateFree>
void AsOfJoinPhysicalOperator<PredicateFree>::releasePending(
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

template <bool PredicateFree>
void AsOfJoinPhysicalOperator<PredicateFree>::compactRightState(
    ExecutionContext& executionCtx, const nautilus::val<Timestamp>& watermark) const
{
    if (watermark == nautilus::val<Timestamp>{Timestamp{Timestamp::INITIAL_VALUE}})
    {
        return;
    }

    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto buffer = invoke(
        getRightBuffer,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{rightTupleLayout->getSchema().getSizeInBytes()});
    const PagedVectorRef rightState{BorrowedNautilusBuffer::from(buffer), rightTupleLayout};
    const auto numberOfRightRows = rightState.getNumberOfRecords();
    const auto watermarkValue = watermark.convertToValue();

    if constexpr (!PredicateFree)
    {
        const auto& keyField = rightKeyField.value();
        /// ponytail: quadratic compaction avoids a second generic key index; replace it with keyed state if profiling warrants it.
        const auto shouldRetain = [&](const nautilus::val<uint64_t>& index)
        {
            const auto record = rightState.at(index);
            const auto& key = record.read(keyField);
            const auto timestamp = invoke(
                +[](OperatorHandler* ptr, const uint64_t pos)
                { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableTimestamp(pos); },
                handler,
                index);
            auto retain = !key.isNull();
            for (nautilus::val<uint64_t> candidateIndex = 0; candidateIndex < numberOfRightRows; ++candidateIndex)
            {
                const auto candidateTimestamp = invoke(
                    +[](OperatorHandler* ptr, const uint64_t pos)
                    { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableTimestamp(pos); },
                    handler,
                    candidateIndex);
                const auto candidateRecord = rightState.at(candidateIndex);
                const auto keyEquality = key == candidateRecord.read(keyField);
                const auto sameKey = !keyEquality.isNull() && keyEquality.getRawValueAs<nautilus::val<bool>>();
                const auto supersedes = candidateTimestamp <= watermarkValue
                    && (candidateTimestamp > timestamp || (candidateTimestamp == timestamp && candidateIndex < index)) && sameKey;
                retain = retain && !(timestamp <= watermarkValue && supersedes);
            }
            return retain;
        };

        nautilus::val<uint64_t> retainedRows = 0;
        for (nautilus::val<uint64_t> index = 0; index < numberOfRightRows; ++index)
        {
            retainedRows = retainedRows + nautilus::select(shouldRetain(index), nautilus::val<uint64_t>{1}, nautilus::val<uint64_t>{0});
        }
        if (retainedRows == numberOfRightRows)
        {
            return;
        }

        const auto compactedBuffer = invoke(
            beginRightCompaction,
            handler,
            executionCtx.pipelineMemoryProvider.bufferProvider,
            nautilus::val<uint64_t>{rightTupleLayout->getSchema().getSizeInBytes()});
        PagedVectorRef compactedRightState{BorrowedNautilusBuffer::from(compactedBuffer), rightTupleLayout};
        for (nautilus::val<uint64_t> index = 0; index < numberOfRightRows; ++index)
        {
            if (shouldRetain(index))
            {
                compactedRightState.pushBack(rightState.at(index), executionCtx.pipelineMemoryProvider.bufferProvider);
                const auto timestamp = invoke(
                    +[](OperatorHandler* ptr, const uint64_t pos)
                    { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableTimestamp(pos); },
                    handler,
                    index);
                invoke(
                    +[](OperatorHandler* ptr, const uint64_t retainedTimestamp)
                    { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).appendCompactedTableTimestamp(retainedTimestamp); },
                    handler,
                    timestamp);
            }
        }
        invoke(+[](OperatorHandler* ptr) { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).finishTableCompaction(); }, handler);
        return;
    }

    nautilus::val<uint64_t> retainedPredecessor = nautilus::val<uint64_t>{UINT64_MAX};
    nautilus::val<uint64_t> retainedTimestamp = 0;
    nautilus::val<uint64_t> numberOfFutureRows = 0;
    for (nautilus::val<uint64_t> index = 0; index < numberOfRightRows; ++index)
    {
        const auto timestamp = invoke(
            +[](OperatorHandler* ptr, const uint64_t pos)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableTimestamp(pos); },
            handler,
            index);
        const auto retain
            = timestamp <= watermarkValue && (retainedPredecessor == nautilus::val<uint64_t>{UINT64_MAX} || timestamp > retainedTimestamp);
        retainedTimestamp = nautilus::select(retain, timestamp, retainedTimestamp);
        retainedPredecessor = nautilus::select(retain, index, retainedPredecessor);
        numberOfFutureRows
            = numberOfFutureRows + nautilus::select(timestamp > watermarkValue, nautilus::val<uint64_t>{1}, nautilus::val<uint64_t>{0});
    }
    if (numberOfFutureRows
            + nautilus::select(
                retainedPredecessor != nautilus::val<uint64_t>{UINT64_MAX}, nautilus::val<uint64_t>{1}, nautilus::val<uint64_t>{0})
        == numberOfRightRows)
    {
        return;
    }

    const auto compactedBuffer = invoke(
        beginRightCompaction,
        handler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{rightTupleLayout->getSchema().getSizeInBytes()});
    PagedVectorRef compactedRightState{BorrowedNautilusBuffer::from(compactedBuffer), rightTupleLayout};
    for (nautilus::val<uint64_t> index = 0; index < numberOfRightRows; ++index)
    {
        const auto record = rightState.at(index);
        const auto timestamp = invoke(
            +[](OperatorHandler* ptr, const uint64_t pos)
            { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableTimestamp(pos); },
            handler,
            index);
        if (index == retainedPredecessor || timestamp > watermarkValue)
        {
            compactedRightState.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);
            invoke(
                +[](OperatorHandler* ptr, const uint64_t retainedTimestamp)
                { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).appendCompactedTableTimestamp(retainedTimestamp); },
                handler,
                timestamp);
        }
    }
    invoke(+[](OperatorHandler* ptr) { dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).finishTableCompaction(); }, handler);
}

template <bool PredicateFree>
void AsOfJoinPhysicalOperator<PredicateFree>::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
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
        const auto previousRightWatermark = invoke(
            +[](OperatorHandler* ptr) { return dynamic_cast<StreamTableJoinOperatorHandler&>(*ptr).getTableWatermark(); }, handler);
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
        if (rightWatermark > previousRightWatermark)
        {
            releasePending(executionCtx, rightWatermark, nautilus::val<bool>{false});
        }
        compactRightState(executionCtx, executionCtx.watermarkTs);
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
        compactRightState(executionCtx, executionCtx.watermarkTs);
    }
    invoke(unlockHandler, handler);
    closeChild(executionCtx, recordBuffer);
}

template <bool PredicateFree>
void AsOfJoinPhysicalOperator<PredicateFree>::terminate(ExecutionContext& executionCtx) const
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

template <bool PredicateFree>
std::optional<PhysicalOperator> AsOfJoinPhysicalOperator<PredicateFree>::getChild() const
{
    return child;
}

template <bool PredicateFree>
void AsOfJoinPhysicalOperator<PredicateFree>::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

template class AsOfJoinPhysicalOperator<true>;
template class AsOfJoinPhysicalOperator<false>;

}
