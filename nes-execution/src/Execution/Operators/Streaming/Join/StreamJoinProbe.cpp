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

#include <ranges>

#include <API/AttributeField.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinProbe.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Util.hpp>
#include <nautilus/std/ostream.h>

namespace NES::Runtime::Execution::Operators
{
void garbageCollectSlices(
    OperatorHandler* ptrOpHandler,
    const Timestamp::Underlying watermarkTs,
    const SequenceNumber::Underlying sequenceNumber,
    const ChunkNumber::Underlying chunkNumber,
    const bool lastChunk,
    const OriginId::Underlying originId)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");

    auto* opHandler = dynamic_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    const BufferMetaData bufferMetaData(
        watermarkTs, SequenceData(SequenceNumber(sequenceNumber), ChunkNumber(chunkNumber), lastChunk), OriginId(originId));
    opHandler->garbageCollectSlicesAndWindows(bufferMetaData);
}

void deleteAllSlicesAndWindowsProxy(OperatorHandler* ptrOpHandler)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");

    auto* opHandler = dynamic_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    opHandler->deleteAllSlicesAndWindows();
}

StreamJoinProbe::StreamJoinProbe(
    const uint64_t operatorHandlerIndex,
    const std::shared_ptr<Functions::Function>& joinFunction,
    const WindowMetaData& windowMetaData,
    const JoinSchema& joinSchema)
    : operatorHandlerIndex(operatorHandlerIndex), joinFunction(joinFunction), windowMetaData(windowMetaData), joinSchema(joinSchema)
{
}

void StreamJoinProbe::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Update the watermark for the nlj probe and delete all slices that can be deleted
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(
        garbageCollectSlices,
        operatorHandlerMemRef,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);

    /// Now close for all children
    Operator::close(executionCtx, recordBuffer);
}

void StreamJoinProbe::terminate(ExecutionContext& executionCtx) const
{
    /// Delete all slices as the query has ended
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(deleteAllSlicesAndWindowsProxy, operatorHandlerMemRef);

    /// Now terminate for all children
    Operator::terminate(executionCtx);
}

Record StreamJoinProbe::createJoinedRecord(
    const Record& leftRecord,
    const Record& rightRecord,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const std::vector<Record::RecordFieldIdentifier>& projectionsLeft,
    const std::vector<Record::RecordFieldIdentifier>& projectionsRight) const
{
    Record joinedRecord;

    /// Writing the window start, end, and key field
    joinedRecord.write(windowMetaData.windowStartFieldName, windowStart.convertToValue());
    joinedRecord.write(windowMetaData.windowEndFieldName, windowEnd.convertToValue());

    /// Writing the leftSchema fields, expect the join schema to have the fields in the same order then the left schema
    for (const auto& fieldName : nautilus::static_iterable(projectionsLeft))
    {
        joinedRecord.write(fieldName, leftRecord.read(fieldName));
    }

    /// Writing the rightSchema fields, expect the join schema to have the fields in the same order then the right schema
    for (const auto& fieldName : nautilus::static_iterable(projectionsRight))
    {
        joinedRecord.write(fieldName, rightRecord.read(fieldName));
    }

    return joinedRecord;
}

Record StreamJoinProbe::createJoinedRecord(
    const Record& leftRecord,
    const Record& rightRecord,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    return createJoinedRecord(
        leftRecord, rightRecord, windowStart, windowEnd, joinSchema.leftSchema->getFieldNames(), joinSchema.rightSchema->getFieldNames());
}
}
