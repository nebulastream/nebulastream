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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJProbeVarSized.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJSliceVarSized.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>

namespace NES::Runtime::Execution::Operators {

uint64_t getHJWindowStartVarSizedProxy(void* ptrJoinPartitionIdSliceId) {
    NES_ASSERT2_FMT(ptrJoinPartitionIdSliceId != nullptr, "join partition id slice id should not be null");
    auto probeInfo = static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId);
    NES_INFO("Probing for leftId {} rightId {} partitionId {} windowStart {} windowEnd {}",
             probeInfo->sliceIdentifierLeft,
             probeInfo->sliceIdentifierRight,
             probeInfo->partitionId,
             probeInfo->windowInfo.windowStart,
             probeInfo->windowInfo.windowEnd);

    return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->windowInfo.windowStart;
}

uint64_t getHJWindowEndVarSizedProxy(void* ptrJoinPartitionIdSliceId) {
    NES_ASSERT2_FMT(ptrJoinPartitionIdSliceId != nullptr, "join partition id slice id should not be null");
    return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->windowInfo.windowEnd;
}

uint64_t getPartitionIdVarSizedProxy(void* ptrJoinPartitionIdSliceId) {
    NES_ASSERT2_FMT(ptrJoinPartitionIdSliceId != nullptr, "ptrJoinPartitionIdSliceId should not be null");
    return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->partitionId;
}

uint64_t getHJSliceIdVarSizedProxy(void* ptrJoinPartitionIdSliceId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrJoinPartitionIdSliceId != nullptr, "ptrJoinPartitionIdSliceId should not be null");
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();

    switch (joinBuildSide) {
        case QueryCompilation::JoinBuildSideType::Left:
            return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->sliceIdentifierLeft;
        case QueryCompilation::JoinBuildSideType::Right:
            return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->sliceIdentifierRight;
    }
}

void* getHashSliceRefFromIdVarSizedProxy(void* ptrOpHandler, uint64_t sliceIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<HJOperatorHandler*>(ptrOpHandler);
    return opHandler->getSliceBySliceIdentifier(sliceIdentifier).value().get();
}

void* getHJBucketAtPosVarSizedProxy(void* hashSlicePtr, uint64_t joinBuildSideInt, uint64_t bucketPos) {
    NES_ASSERT2_FMT(hashSlicePtr != nullptr, "hashSlicePtr should not be null");
    auto hashSlice = static_cast<HJSliceVarSized*>(hashSlicePtr);
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    NES_INFO("Probing for {} tuples {} with start {} end {}",
             magic_enum::enum_name(joinBuildSide),
             hashSlice->getMergingHashTable(joinBuildSide).getNumItems(bucketPos),
             hashSlice->getSliceStart(),
             hashSlice->getSliceEnd());
    return hashSlice->getMergingHashTable(joinBuildSide).getBucketAtPos(bucketPos);
}

void HJProbeVarSized::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // As this operator functions as a scan, we have to set the execution context for this pipeline
    ctx.setWatermarkTs(recordBuffer.getWatermarkTs());
    ctx.setSequenceNumber(recordBuffer.getSequenceNr());
    ctx.setChunkNumber(recordBuffer.getChunkNr());
    ctx.setLastChunk(recordBuffer.isLastChunk());
    ctx.setOrigin(recordBuffer.getOriginId());
    Operator::open(ctx, recordBuffer);

    // Getting all needed references and values
    const auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto joinPartitionIdSliceIdMemRef = recordBuffer.getBuffer();

    const auto windowStart =
        FunctionCall("getHJWindowStartVarSizedProxy", getHJWindowStartVarSizedProxy, joinPartitionIdSliceIdMemRef);
    const auto windowEnd = FunctionCall("getHJWindowEndVarSizedProxy", getHJWindowEndVarSizedProxy, joinPartitionIdSliceIdMemRef);
    const auto partitionId =
        FunctionCall("getPartitionIdVarSizedProxy", getPartitionIdVarSizedProxy, joinPartitionIdSliceIdMemRef);

    const auto sliceIdLeft = FunctionCall("getHJSliceIdVarSizedProxy",
                                          getHJSliceIdVarSizedProxy,
                                          joinPartitionIdSliceIdMemRef,
                                          Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)));
    const auto sliceIdRight = FunctionCall("getHJSliceIdVarSizedProxy",
                                           getHJSliceIdVarSizedProxy,
                                           joinPartitionIdSliceIdMemRef,
                                           Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)));

    // During triggering the slice, we append all pages of all local copies to a single PagedVector located at position 0
    const ValueId<WorkerThreadId> workerThreadIdForPagedVectors(INITIAL<WorkerThreadId>);

    const auto hashSliceRefLeft = FunctionCall("getHashSliceRefFromIdVarSizedProxy",
                                               getHashSliceRefFromIdVarSizedProxy,
                                               operatorHandlerMemRef,
                                               sliceIdLeft);
    const auto hashSliceRefRight = FunctionCall("getHashSliceRefFromIdVarSizedProxy",
                                                getHashSliceRefFromIdVarSizedProxy,
                                                operatorHandlerMemRef,
                                                sliceIdRight);

    const auto leftPagedVectorRef = FunctionCall("getHJBucketAtPosVarSizedProxy",
                                                 getHJBucketAtPosVarSizedProxy,
                                                 hashSliceRefLeft,
                                                 Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)),
                                                 partitionId);
    const auto rightPagedVectorRef = FunctionCall("getHJBucketAtPosVarSizedProxy",
                                                  getHJBucketAtPosVarSizedProxy,
                                                  hashSliceRefRight,
                                                  Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)),
                                                  partitionId);

    Interface::PagedVectorVarSizedRef leftPagedVector(leftPagedVectorRef, leftSchema);
    Interface::PagedVectorVarSizedRef rightPagedVector(rightPagedVectorRef, rightSchema);
    const auto leftNumberOfEntries = leftPagedVector.getTotalNumberOfEntries();
    const auto rightNumberOfEntries = rightPagedVector.getTotalNumberOfEntries();

    for (Value<UInt64> leftCnt = 0_u64; leftCnt < leftNumberOfEntries; leftCnt = leftCnt + 1) {
        for (Value<UInt64> rightCnt = 0_u64; rightCnt < rightNumberOfEntries; rightCnt = rightCnt + 1) {
            auto leftRecord = leftPagedVector.readRecord(leftCnt);
            auto rightRecord = rightPagedVector.readRecord(rightCnt);

            Record joinedRecord;
            createJoinedRecord(joinedRecord, leftRecord, rightRecord, windowStart, windowEnd);
            if (joinExpression->execute(joinedRecord).as<Boolean>()) {
                // Calling the child operator for this joinedRecord
                child->execute(ctx, joinedRecord);
            }
        }
    }
}

HJProbeVarSized::HJProbeVarSized(const uint64_t operatorHandlerIndex,
                                 const JoinSchema& joinSchema,
                                 Expressions::ExpressionPtr joinExpression,
                                 const WindowMetaData& windowMetaData,
                                 const SchemaPtr& leftSchema,
                                 const SchemaPtr& rightSchema,
                                 QueryCompilation::StreamJoinStrategy joinStrategy,
                                 QueryCompilation::WindowingStrategy windowingStrategy,
                                 bool withDeletion)
    : StreamJoinProbe(operatorHandlerIndex,
                      joinSchema,
                      joinExpression,
                      windowMetaData,
                      joinStrategy,
                      windowingStrategy,
                      withDeletion),
      leftSchema(leftSchema), rightSchema(rightSchema) {}

}// namespace NES::Runtime::Execution::Operators
