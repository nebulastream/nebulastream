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
#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperator.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinProbe.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Deletes all slices that are not valid anymore
 */
void deleteAllSlicesProxy(void* ptrOpHandler,
                          uint64_t watermarkTs,
                          uint64_t sequenceNumber,
                          uint64_t chunkNumber,
                          bool lastChunk,
                          OriginId originId,
                          uint64_t joinStrategyInt,
                          uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = StreamJoinOperator::getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    BufferMetaData bufferMetaData(watermarkTs, {sequenceNumber, chunkNumber, lastChunk}, originId);
    opHandler->deleteSlices(bufferMetaData);
}

void deleteAllWindowsProxy(void* ptrOpHandler, void* ptrPipelineCtx, uint64_t joinStrategyInt, uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");

    auto* opHandler = StreamJoinOperator::getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    NES_DEBUG("Deleting all slices for pipelineId {}!", pipelineCtx->getPipelineID());

    opHandler->deleteAllSlices();
}

void StreamJoinProbe::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    if (withDeletion) {
        // Update the watermark for the nlj probe and delete all slices that can be deleted
        const auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
        Nautilus::FunctionCall("deleteAllSlicesProxy",
                               deleteAllSlicesProxy,
                               operatorHandlerMemRef,
                               ctx.getWatermarkTs(),
                               ctx.getSequenceNumber(),
                               ctx.getChunkNumber(),
                               ctx.getLastChunk(),
                               ctx.getOriginId(),
                               Value<UInt64>(to_underlying<QueryCompilation::StreamJoinStrategy>(joinStrategy)),
                               Value<UInt64>(to_underlying<QueryCompilation::WindowingStrategy>(windowingStrategy)));
    }

    // Now close for all children
    Operator::close(ctx, recordBuffer);
}

void StreamJoinProbe::createJoinedRecord(Record& joinedRecord,
                                         Record& leftRecord,
                                         Record& rightRecord,
                                         const Value<UInt64>& windowStart,
                                         const Value<UInt64>& windowEnd) const {
    // Writing the window start, end, and key field
    joinedRecord.write(windowMetaData.windowStartFieldName, windowStart);
    joinedRecord.write(windowMetaData.windowEndFieldName, windowEnd);
    joinedRecord.write(windowMetaData.windowKeyFieldName, leftRecord.read(joinFieldNameLeft));

    /* Writing the leftSchema fields, expect the join schema to have the fields in the same order then the left schema */
    for (auto& field : joinSchema.leftSchema->fields) {
        joinedRecord.write(field->getName(), leftRecord.read(field->getName()));
    }

    /* Writing the rightSchema fields, expect the join schema to have the fields in the same order then the right schema */
    for (auto& field : joinSchema.rightSchema->fields) {
        joinedRecord.write(field->getName(), rightRecord.read(field->getName()));
    }
}

StreamJoinProbe::StreamJoinProbe(const uint64_t operatorHandlerIndex,
                                 const JoinSchema& joinSchema,
                                 std::string joinFieldNameLeft,
                                 std::string joinFieldNameRight,
                                 const WindowMetaData& windowMetaData,
                                 QueryCompilation::StreamJoinStrategy joinStrategy,
                                 QueryCompilation::WindowingStrategy windowingStrategy,
                                 bool withDeletion)
    : StreamJoinOperator(joinStrategy, windowingStrategy), operatorHandlerIndex(operatorHandlerIndex), joinSchema(joinSchema),
      withDeletion(withDeletion), joinFieldNameLeft(std::move(joinFieldNameLeft)),
      joinFieldNameRight(std::move(joinFieldNameRight)), windowMetaData(windowMetaData) {}

void StreamJoinProbe::terminate(ExecutionContext& ctx) const {
    // Trigger all slices, as the query has ended
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("deleteAllWindowsProxy",
                           deleteAllWindowsProxy,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           Value<UInt64>(to_underlying<QueryCompilation::StreamJoinStrategy>(joinStrategy)),
                           Value<UInt64>(to_underlying<QueryCompilation::WindowingStrategy>(windowingStrategy)));
    Operator::terminate(ctx);
}

}// namespace NES::Runtime::Execution::Operators