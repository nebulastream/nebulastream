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

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJProbe.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* getNLJSliceRefFromIdProxy(void* ptrOpHandler, uint64_t sliceIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto opHandler = static_cast<NLJOperatorHandlerSlicing*>(ptrOpHandler);
    auto slice = opHandler->getSliceBySliceIdentifier(sliceIdentifier);
    if (slice.has_value()) {
        return slice.value().get();
    }
    // For now this is fine. We should handle this as part of issue #4016
    NES_ERROR("Could not find a slice with the id: {}", sliceIdentifier);
    return nullptr;
}

uint64_t getNLJWindowStartProxy(void* ptrNLJWindowTriggerTask) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    return static_cast<EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask)->windowInfo.windowStart;
}

uint64_t getNLJWindowEndProxy(void* ptrNLJWindowTriggerTask) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    return static_cast<EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask)->windowInfo.windowEnd;
}

uint64_t getSliceIdNLJProxy(void* ptrNLJWindowTriggerTask, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();

    if (joinBuildSide == QueryCompilation::JoinBuildSideType::Left) {
        return static_cast<EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask)->leftSliceIdentifier;
    } else if (joinBuildSide == QueryCompilation::JoinBuildSideType::Right) {
        return static_cast<EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask)->rightSliceIdentifier;
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

void NLJProbe::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // As this operator functions as a scan, we have to set the execution context for this pipeline
    ctx.setWatermarkTs(recordBuffer.getWatermarkTs());
    ctx.setSequenceNumber(recordBuffer.getSequenceNr());
    ctx.setChunkNumber(recordBuffer.getChunkNr());
    ctx.setLastChunk(recordBuffer.isLastChunk());
    ctx.setOrigin(recordBuffer.getOriginId());
    Operator::open(ctx, recordBuffer);

    // Getting all needed info from the recordBuffer
    const auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto nljWindowTriggerTaskRef = recordBuffer.getBuffer();
    const Value<UInt64> sliceIdLeft =
        Nautilus::FunctionCall("getSliceIdNLJProxy",
                               getSliceIdNLJProxy,
                               nljWindowTriggerTaskRef,
                               Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)));
    const Value<UInt64> sliceIdRight =
        Nautilus::FunctionCall("getSliceIdNLJProxy",
                               getSliceIdNLJProxy,
                               nljWindowTriggerTaskRef,
                               Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)));
    const auto windowStart = Nautilus::FunctionCall("getNLJWindowStartProxy", getNLJWindowStartProxy, nljWindowTriggerTaskRef);
    const auto windowEnd = Nautilus::FunctionCall("getNLJWindowEndProxy", getNLJWindowEndProxy, nljWindowTriggerTaskRef);

    // During triggering the slice, we append all pages of all local copies to a single PagedVector located at position 0
    const ValueId<WorkerThreadId> workerThreadIdForPages = WorkerThreadId(0);

    // Getting the left and right paged vector
    const auto sliceRefLeft =
        Nautilus::FunctionCall("getNLJSliceRefFromIdProxy", getNLJSliceRefFromIdProxy, operatorHandlerMemRef, sliceIdLeft);
    const auto sliceRefRight =
        Nautilus::FunctionCall("getNLJSliceRefFromIdProxy", getNLJSliceRefFromIdProxy, operatorHandlerMemRef, sliceIdRight);
    const auto leftPagedVectorRef =
        Nautilus::FunctionCall("getNLJPagedVectorProxy",
                               getNLJPagedVectorProxy,
                               sliceRefLeft,
                               workerThreadIdForPages,
                               Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)));
    const auto rightPagedVectorRef =
        Nautilus::FunctionCall("getNLJPagedVectorProxy",
                               getNLJPagedVectorProxy,
                               sliceRefRight,
                               workerThreadIdForPages,
                               Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)));

    Nautilus::Interface::PagedVectorVarSizedRef leftPagedVector(leftPagedVectorRef, leftSchema);
    Nautilus::Interface::PagedVectorVarSizedRef rightPagedVector(rightPagedVectorRef, rightSchema);

    auto const leftSliceStart = Nautilus::FunctionCall("getNLJSliceStartProxy", getNLJSliceStartProxy, sliceRefLeft);
    auto const leftSliceEnd = Nautilus::FunctionCall("getNLJSliceEndProxy", getNLJSliceEndProxy, sliceRefLeft);
    auto const rightSliceStart = Nautilus::FunctionCall("getNLJSliceStartProxy", getNLJSliceStartProxy, sliceRefRight);
    auto const rightSliceEnd = Nautilus::FunctionCall("getNLJSliceEndProxy", getNLJSliceEndProxy, sliceRefRight);

    auto leftSliceNotCompletelyContained = leftSliceStart < windowStart || leftSliceEnd > windowEnd;
    auto rightSliceNotCompletelyContained = rightSliceStart < windowStart || rightSliceEnd > windowEnd;

    const auto leftNumberOfEntries = leftPagedVector.getTotalNumberOfEntries();
    const auto rightNumberOfEntries = rightPagedVector.getTotalNumberOfEntries();

    NES_INFO("current probe: left Slice({}) {}, {}, #{}; right Slice({}) {}, {}, #{}; window {}, {}",
             sliceIdLeft->getValue(),
             leftSliceStart->getValue(),
             leftSliceEnd->getValue(),
             leftNumberOfEntries->getValue(),
             sliceIdRight->getValue(),
             rightSliceStart->getValue(),
             rightSliceEnd->getValue(),
             rightNumberOfEntries->getValue(),
             windowStart->getValue(),
             windowEnd->getValue())

    bool skippedRightRecordsLogged = false;
    bool helperSkippedBool = false;
    for (Value<UInt64> leftCnt = 0_u64; leftCnt < leftNumberOfEntries; leftCnt = leftCnt + 1) {
        auto leftRecord = leftPagedVector.readRecord(leftCnt);
        if (leftSliceNotCompletelyContained
            && (leftTimeFunctionPtr->getTsWithoutContext(leftRecord) < windowStart
                || leftTimeFunctionPtr->getTsWithoutContext(leftRecord) >= windowEnd)) {
            NES_INFO("skipped left record of slice that was adjusted. Record ts is {}, but window starts at {} and ends at {}",
                     leftTimeFunctionPtr->getTsWithoutContext(leftRecord)->getValue(),
                     windowStart->getValue(),
                     windowEnd->getValue())
            continue;
        }
        if (helperSkippedBool) {
            skippedRightRecordsLogged = true;
        }

        for (Value<UInt64> rightCnt = 0_u64; rightCnt < rightNumberOfEntries; rightCnt = rightCnt + 1) {
            auto rightRecord = rightPagedVector.readRecord(rightCnt);
            if (rightSliceNotCompletelyContained
                && (rightTimeFunctionPtr->getTsWithoutContext(rightRecord) < windowStart
                    || rightTimeFunctionPtr->getTsWithoutContext(rightRecord) >= windowEnd)) {
                if (!skippedRightRecordsLogged) {
                    NES_INFO("skipped right record of slice that was adjusted. Record ts is {}, but window starts at {} and ends "
                             "at {}",
                             rightTimeFunctionPtr->getTsWithoutContext(rightRecord)->getValue(),
                             windowStart->getValue(),
                             windowEnd->getValue())
                    helperSkippedBool = true;
                }
                continue;
            }

            Record joinedRecord;
            createJoinedRecord(joinedRecord, leftRecord, rightRecord, windowStart, windowEnd);
            NES_ERROR("created joined record for {} - {} and {}: {}", windowStart->getValue(), windowEnd->getValue(), leftNumberOfEntries->getValue(), rightNumberOfEntries->getValue());
            if (joinExpression->execute(joinedRecord).as<Boolean>()) {
                child->execute(ctx, joinedRecord);
            }
        }
    }
}

NLJProbe::NLJProbe(const uint64_t operatorHandlerIndex,
                   const JoinSchema& joinSchema,
                   const Expressions::ExpressionPtr joinExpression,
                   const WindowMetaData& windowMetaData,
                   const SchemaPtr& leftSchema,
                   const SchemaPtr& rightSchema,
                   QueryCompilation::StreamJoinStrategy joinStrategy,
                   QueryCompilation::WindowingStrategy windowingStrategy,
                   TimeFunctionPtr leftTimeFunctionPtr,
                   TimeFunctionPtr rightTimeFunctionPtr,
                   bool withDeletion)
    : StreamJoinProbe(operatorHandlerIndex,
                      joinSchema,
                      std::move(joinExpression),
                      windowMetaData,
                      joinStrategy,
                      windowingStrategy,
                      std::move(leftTimeFunctionPtr),
                      std::move(rightTimeFunctionPtr),
                      withDeletion),
      leftSchema(leftSchema), rightSchema(rightSchema) {}

};// namespace NES::Runtime::Execution::Operators
