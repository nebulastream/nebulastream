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
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJInterval.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJProbe.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * START OF PROXY FUNCTIONS (FUNCTION CALLS)
 */

uint64_t getIntervalIdIJProxy(void* ptrIJIntervalTriggerTask) {
    NES_ASSERT2_FMT(ptrIJIntervalTriggerTask != nullptr, "ptrIJIntervalTriggerTask should not be null");
    return static_cast<EmittedIJTriggerTask*>(ptrIJIntervalTriggerTask)->intervalIdentifier;
}

void* getIJIntervalRefFromIdProxy(void* ptrOpHandler, uint64_t intervalIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto opHandler = static_cast<IJOperatorHandler*>(ptrOpHandler);
    auto interval = opHandler->getIntervalByIntervalIdentifier(intervalIdentifier);
    if (interval.has_value()) {
        return interval.value().get();
    }
    // For now nullptr return is fine. We should handle this as part of issue #4016
    NES_ERROR("Could not find an interval with the id: {}", intervalIdentifier);
    return nullptr;
}

/**
 * checks if intervals have been emitted and processed so that right tuples can be cleaned
 */

bool checkIfCleanIsOutstandingProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto opHandler = static_cast<IJOperatorHandler*>(ptrOpHandler);
    return opHandler->getSmallestIntervalStartSeen() > opHandler->getLatestRightTupleBufferCleanTimeStamp();
}

void* getLeftIntervalPagedVectorProxy(void* intervalPtr) {
    NES_ASSERT2_FMT(intervalPtr != nullptr, "intervalPtr should not be null");
    auto* intervalPtrCast = static_cast<IJInterval*>(intervalPtr);
    return intervalPtrCast->getPagedVectorRefLeft();
}

void* getRightIntervalPagedVectorProxy(void* intervalPtr, WorkerThreadId workerThreadId) {
    NES_ASSERT2_FMT(intervalPtr != nullptr, "intervalPtr should not be null");
    auto* intervalPtrCast = static_cast<IJInterval*>(intervalPtr);
    return intervalPtrCast->getPagedVectorRefRight(workerThreadId);
}

int64_t getIntervalStartProxy(void* intervalPtr) {
    NES_ASSERT2_FMT(intervalPtr != nullptr, "intervalPtr should not be null");
    auto* intervalPtrCast = static_cast<IJInterval*>(intervalPtr);
    return intervalPtrCast->getIntervalStart();
}

int64_t getIntervalEndProxy(void* intervalPtr) {
    NES_ASSERT2_FMT(intervalPtr != nullptr, "intervalPtr should not be null");
    auto* intervalPtrCast = static_cast<IJInterval*>(intervalPtr);
    return intervalPtrCast->getIntervalEnd();
}

void deleteAllIntervalsProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    const auto opHandler = static_cast<IJOperatorHandler*>(ptrOpHandler);
    opHandler->deleteAllIntervals();
    NES_DEBUG("Deleted all intervals!");
}

/**
 * @brief: sets the vector of right PageVectors for the operatorHandler to the newRightPagedVectorPtr
 */
void setRightHandlerPagedVectorProxy(void* operatorHandlerPtr, int64_t timestamp) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    opHandler->updateRightTuples(timestamp);
}

/**
 * @brief Deletes an executed interval
 */
extern "C" void removeIJIntervalRefFromOpHandlerProxy(void* ptrOpHandler, uint64_t intervalIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto opHandler = static_cast<IJOperatorHandler*>(ptrOpHandler);
    auto interval = opHandler->getIntervalByIntervalIdentifier(intervalIdentifier);
    interval->get()->intervalState = IJIntervalInfoState::MARKED_FOR_DELETION;
}

/**
 * calculate the expiration time of an right tuple, i.e., when no interval occurs anymore the right tuple might be added to
 */
int64_t getExpirationTimeProxy(void* operatorHandlerPtr) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    return ijOperatorHandler->getSmallestIntervalStartSeen();
}

/**
 * @brief checks if vector of rightPagedVector is empty and if so adds one pageVector
 */
void* createNewVectorOfPageVectorsProxy(void* operatorHandlerPtr, void* pipCtx) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(pipCtx);
    auto bufferManager = pipelineCtx->getBufferManager();
    ijOperatorHandler->getUpdatedRightTuples().clear();
    // create a new vector
    ijOperatorHandler->getUpdatedRightTuples().emplace_back(
        std::make_unique<Nautilus::Interface::PagedVectorVarSized>(bufferManager,
                                                                   ijOperatorHandler->getRightSchema(),
                                                                   ijOperatorHandler->getPageSizeRight()));

    return ijOperatorHandler->getUpdatedRightTuples()[0].get();
}

/**
 * @brief Proxy function for returning the pointer to the requested right tuple PagedVector from the operatorHandler
 */
void* getRightPagedVectorFromOperatorHandlerProbeProxy(void* operatorHandlerPtr, WorkerThreadId workerThreadId) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    return ijOperatorHandler->getPagedVectorRefRight(workerThreadId);
}

/**
 * END OF PROXY FUNCTIONS (FUNCTION CALLS)
 */

IJProbe::IJProbe(const uint64_t operatorHandlerIndex,
                 Expressions::ExpressionPtr joinExpression,
                 const std::string& intervalStartField,
                 const std::string& intervalEndField,
                 TimeFunctionPtr timeFunction,
                 const SchemaPtr& leftSchema,
                 const SchemaPtr& rightSchema)
    : operatorHandlerIndex(operatorHandlerIndex), leftSchema(leftSchema), rightSchema(rightSchema),
      joinExpression(joinExpression), intervalStartField(std::move(intervalStartField)),
      intervalEndField(std::move(intervalEndField)), timeFunction(std::move(timeFunction)) {}

void IJProbe::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    ctx.setWatermarkTs(recordBuffer.getWatermarkTs());
    ctx.setSequenceNumber(recordBuffer.getSequenceNr());
    ctx.setChunkNumber(recordBuffer.getChunkNr());
    ctx.setLastChunk(recordBuffer.isLastChunk());
    ctx.setOrigin(recordBuffer.getOriginId());
    Operator::open(ctx, recordBuffer);

    const auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto emittedIJTriggerTask = recordBuffer.getBuffer();
    // we separate the three following function calls as we need the intervalId and the intervalMemRef separate later on
    const Value<UInt64> intervalId = Nautilus::FunctionCall("getIntervalIdIJProxy", getIntervalIdIJProxy, emittedIJTriggerTask);

    // During triggering the interval, we append all pages of all local copies to a single PagedVector located at position 0
    const ValueId<WorkerThreadId> workerThreadIdForPages = WorkerThreadId(0);

    const auto intervalMemRef =
        Nautilus::FunctionCall("getIJIntervalRefFromIdProxy", getIJIntervalRefFromIdProxy, operatorHandlerMemRef, intervalId);

    // Getting the left and right paged vector
    const auto leftIntervalPagedVector =
        Nautilus::FunctionCall("getLeftIntervalPagedVectorProxy", getLeftIntervalPagedVectorProxy, intervalMemRef);

    const auto rightIntervalPagedVector = Nautilus::FunctionCall("getRightIntervalPagedVectorProxy",
                                                                 getRightIntervalPagedVectorProxy,
                                                                 intervalMemRef,
                                                                 workerThreadIdForPages);

    Nautilus::Interface::PagedVectorVarSizedRef leftPagedVector(leftIntervalPagedVector, leftSchema);
    Nautilus::Interface::PagedVectorVarSizedRef rightPagedVector(rightIntervalPagedVector, rightSchema);

    const auto rightNumberOfEntries = rightPagedVector.getTotalNumberOfEntries();
    auto leftRecord =
        leftPagedVector.readRecord(0_u64);// we can use index 0 as we know that the left side is always filled with only one tuple

    for (Value<UInt64> rightCnt = 0_u64; rightCnt < rightNumberOfEntries; rightCnt = rightCnt + 1) {
        auto rightRecord = rightPagedVector.readRecord(rightCnt);
        Record joinedRecord;
        auto intervalStart = FunctionCall("getIntervalStartProxy", getIntervalStartProxy, intervalMemRef);
        auto intervalEnd = FunctionCall("getIntervalEndProxy", getIntervalEndProxy, intervalMemRef);

        createJoinedRecord(joinedRecord, leftRecord, rightRecord, intervalStart, intervalEnd);

        if (joinExpression->execute(joinedRecord).as<Boolean>()) {
            NES_DEBUG("Joined record: {}", joinedRecord.toString());
            // Calling the child operator for this joinedRecord
            child->execute(ctx, joinedRecord);
        }
    }
    FunctionCall("removeIJIntervalRefFromOpHandlerProxy",
                 removeIJIntervalRefFromOpHandlerProxy,
                 operatorHandlerMemRef,
                 intervalId);
}

void IJProbe::createJoinedRecord(Record& joinedRecord,
                                 Record& leftRecord,
                                 Record& rightRecord,
                                 Value<Int64> intervalStart,
                                 Value<Int64> intervalEnd) const {
    joinedRecord.write(intervalStartField, intervalStart);
    joinedRecord.write(intervalEndField, intervalEnd);

    for (auto& field : leftSchema->fields) {
        joinedRecord.write(field->getName(), leftRecord.read(field->getName()));
    }
    for (auto& field : rightSchema->fields) {
        joinedRecord.write(field->getName(), rightRecord.read(field->getName()));
    }
}

void IJProbe::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // this function cleans the page vector of the opHandler from expired tuples, i.e., right tuples that cannot be added to any interval in the future
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    if (Nautilus::FunctionCall("checkIfCleanIsOutstandingProxy", checkIfCleanIsOutstandingProxy, operatorHandlerMemRef)) {
        NES_DEBUG("Clean State with old right tuples")
        auto workerThreadId = ctx.getWorkerThreadId();
        auto pipCtx = ctx.getPipelineContext();

        /**
        * expirationTime = the smallest intervalStart seen in the last emitted phase of the OpHandler
        */
        auto expirationTime = Nautilus::FunctionCall("getExpirationTimeProxy", getExpirationTimeProxy, operatorHandlerMemRef);

        // get the current vector of righten pageVectors of the operator Handler
        auto oldRightPagedVectorRef = Nautilus::FunctionCall("getRightPagedVectorFromOperatorHandlerProbeProxy",
                                                             getRightPagedVectorFromOperatorHandlerProbeProxy,
                                                             operatorHandlerMemRef,
                                                             workerThreadId);
        Nautilus::Interface::PagedVectorVarSizedRef rightHandlerPagedVector(oldRightPagedVectorRef, rightSchema);

        // create a new vector of righten pageVectors
        auto newRightTuplesMemRef = Nautilus::FunctionCall("createNewVectorOfPageVectorsProxy",
                                                           createNewVectorOfPageVectorsProxy,
                                                           operatorHandlerMemRef,
                                                           pipCtx);
        Nautilus::Interface::PagedVectorVarSizedRef newRightPagedVector(newRightTuplesMemRef, rightSchema);
        // check for each right Tuple in the oldPageVector if it is still valid
        const auto rightNumberOfEntries = rightHandlerPagedVector.getTotalNumberOfEntries();
        for (Value<UInt64> rightCnt = 0_u64; rightCnt < rightNumberOfEntries; rightCnt = rightCnt + 1) {
            auto rightRecord = rightHandlerPagedVector.readRecord(rightCnt);
            // if tuple is not expired, write it to the new pagedVector
            auto rightTimestampVal = timeFunction->getTs(ctx, rightRecord);
            if (rightTimestampVal >= expirationTime) {
                newRightPagedVector.writeRecord(rightRecord);
            }
        }
        //replace the old vector of right pagesVectors with the new one
        Nautilus::FunctionCall("setRightHandlerPagedVectorProxy",
                               setRightHandlerPagedVectorProxy,
                               operatorHandlerMemRef,
                               expirationTime);
    }
    // Finally, close for all children
    Operator::close(ctx, recordBuffer);
}

void IJProbe::terminate(ExecutionContext& ctx) const {
    // Delete all slices, as the query has ended
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("deleteAllIntervalsProxy", deleteAllIntervalsProxy, operatorHandlerMemRef);
    Operator::terminate(ctx);
}

};// namespace NES::Runtime::Execution::Operators
