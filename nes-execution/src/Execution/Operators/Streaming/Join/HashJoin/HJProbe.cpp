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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJProbe.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FixedPage/FixedPageRef.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {

uint64_t getWindowStartProxyForHashJoin(void* ptrJoinPartitionIdSliceId) {
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

uint64_t getWindowEndProxyForHashJoin(void* ptrJoinPartitionIdSliceId) {
    NES_ASSERT2_FMT(ptrJoinPartitionIdSliceId != nullptr, "join partition id slice id should not be null");
    return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->windowInfo.windowEnd;
}

uint64_t getPartitionIdProxy(void* ptrJoinPartitionIdSliceId) {
    NES_ASSERT2_FMT(ptrJoinPartitionIdSliceId != nullptr, "ptrJoinPartitionIdSliceId should not be null");
    return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->partitionId;
}

uint64_t getSliceIdHJProxy(void* ptrJoinPartitionIdSliceId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrJoinPartitionIdSliceId != nullptr, "ptrJoinPartitionIdSliceId should not be null");
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();

    switch (joinBuildSide) {
        case QueryCompilation::JoinBuildSideType::Left:
            return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->sliceIdentifierLeft;
        case QueryCompilation::JoinBuildSideType::Right:
            return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->sliceIdentifierRight;
    }
}

void* getHashSliceProxy(void* ptrOpHandler, uint64_t sliceIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<HJOperatorHandler*>(ptrOpHandler);
    return opHandler->getSliceBySliceIdentifier(sliceIdentifier).value().get();
}

void* getPageFromBucketAtPosProxyForHashJoin(void* hashSlicePtr, uint64_t joinBuildSideInt, uint64_t bucketPos, uint64_t pageNo) {
    NES_ASSERT2_FMT(hashSlicePtr != nullptr, "hashSlicePtr should not be null");
    auto hashSlice = static_cast<HJSlice*>(hashSlicePtr);
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    NES_INFO("Probing for {} tuples {} with start {} end {}",
             magic_enum::enum_name(joinBuildSide),
             hashSlice->getMergingHashTable(joinBuildSide).getNumItems(bucketPos),
             hashSlice->getSliceStart(),
             hashSlice->getSliceEnd());
    return hashSlice->getMergingHashTable(joinBuildSide).getPageFromBucketAtPos(bucketPos, pageNo);
}

uint64_t getNumberOfPagesProxyForHashJoin(void* ptrHashSlice, uint64_t joinBuildSideInt, uint64_t bucketPos) {
    NES_ASSERT2_FMT(ptrHashSlice != nullptr, "ptrHashSlice should not be null");
    const auto hashSlice = static_cast<HJSlice*>(ptrHashSlice);
    const auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    auto numPages = hashSlice->getMergingHashTable(joinBuildSide).getNumPages(bucketPos);
    auto numItems = hashSlice->getMergingHashTable(joinBuildSide).getNumItems(bucketPos);

    NES_INFO("numPages: {}, numItems {} for slide start {} end {}",
             numPages,
             numItems,
             hashSlice->getSliceStart(),
             hashSlice->getSliceEnd());
    return numPages;
}

uint64_t getNumberOfTuplesForPageProxy(void* hashWindowPtr, uint64_t joinBuildSideInt, uint64_t bucketPos, uint64_t pageNo) {
    NES_ASSERT2_FMT(hashWindowPtr != nullptr, "hashWindowPtr should not be null");
    const auto hashWindow = static_cast<HJSlice*>(hashWindowPtr);
    const auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    return hashWindow->getMergingHashTable(joinBuildSide).getNumberOfTuplesForPage(bucketPos, pageNo);
}

void HJProbe::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // As this operator functions as a scan, we have to set the execution context for this pipeline
    ctx.setWatermarkTs(recordBuffer.getWatermarkTs());
    ctx.setSequenceNumber(recordBuffer.getSequenceNr());
    ctx.setChunkNumber(recordBuffer.getChunkNr());
    ctx.setLastChunk(recordBuffer.isLastChunk());
    ctx.setOrigin(recordBuffer.getOriginId());
    Operator::open(ctx, recordBuffer);

    // Getting all needed references or values
    const auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto joinPartitionIdSliceIdMemRef = recordBuffer.getBuffer();
    const auto windowStart =
        Nautilus::FunctionCall("getWindowStartProxyForHashJoin", getWindowStartProxyForHashJoin, joinPartitionIdSliceIdMemRef);
    const auto windowEnd =
        Nautilus::FunctionCall("getWindowEndProxyForHashJoin", getWindowEndProxyForHashJoin, joinPartitionIdSliceIdMemRef);
    const auto partitionId = Nautilus::FunctionCall("getPartitionIdProxy", getPartitionIdProxy, joinPartitionIdSliceIdMemRef);
    const auto sliceIdLeft = Nautilus::FunctionCall("getSliceIdHJProxy",
                                                    getSliceIdHJProxy,
                                                    joinPartitionIdSliceIdMemRef,
                                                    Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)));
    const auto sliceIdRight = Nautilus::FunctionCall("getSliceIdHJProxy",
                                                     getSliceIdHJProxy,
                                                     joinPartitionIdSliceIdMemRef,
                                                     Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)));

    const auto hashSliceRefLeft =
        Nautilus::FunctionCall("getHashSliceProxy", getHashSliceProxy, operatorHandlerMemRef, sliceIdLeft);
    const auto hashSliceRefRight =
        Nautilus::FunctionCall("getHashSliceProxy", getHashSliceProxy, operatorHandlerMemRef, sliceIdRight);
    const auto numberOfPagesLeft = Nautilus::FunctionCall("getNumberOfPagesProxyForHashJoin",
                                                          getNumberOfPagesProxyForHashJoin,
                                                          hashSliceRefLeft,
                                                          Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)),
                                                          partitionId);
    const auto numberOfPagesRight =
        Nautilus::FunctionCall("getNumberOfPagesProxyForHashJoin",
                               getNumberOfPagesProxyForHashJoin,
                               hashSliceRefRight,
                               Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)),
                               partitionId);

    //for every left page
    for (Value<UInt64> leftPageNo(0_u64); leftPageNo < numberOfPagesLeft; leftPageNo = leftPageNo + 1) {
        //for every key in left page
        auto leftPageRef = Nautilus::FunctionCall("getPageFromBucketAtPosProxyForHashJoin",
                                                  getPageFromBucketAtPosProxyForHashJoin,
                                                  hashSliceRefLeft,
                                                  Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)),
                                                  partitionId,
                                                  leftPageNo);
        Nautilus::Interface::FixedPageRef leftFixedPageRef(leftPageRef);

        for (auto leftRecordRef : leftFixedPageRef) {
            Value<UInt64> zeroValue = 0_u64;
            auto leftRecord = leftMemProvider->read({}, leftRecordRef, zeroValue);

            //for every right page
            for (Value<UInt64> rightPageNo((uint64_t) 0); rightPageNo < numberOfPagesRight; rightPageNo = rightPageNo + 1) {
                //TODO: introduce Bloomfilter here #3909
                //                if (!rhsPage->bloomFilterCheck(lhsKeyPtr, sizeOfLeftKey)) {
                //                    continue;
                //                }

                //for every key in right page
                auto rightPageRef =
                    Nautilus::FunctionCall("getPageFromBucketAtPosProxyForHashJoin",
                                           getPageFromBucketAtPosProxyForHashJoin,
                                           hashSliceRefRight,
                                           Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)),
                                           partitionId,
                                           rightPageNo);
                Nautilus::Interface::FixedPageRef rightFixedPageRef(rightPageRef);

                for (auto rightRecordRef : rightFixedPageRef) {
                    auto rightRecord = rightMemProvider->read({}, rightRecordRef, zeroValue);
                    if (leftRecord.read(joinFieldNameLeft) == rightRecord.read(joinFieldNameRight)) {
                        Record joinedRecord;
                        StreamJoinProbe::createJoinedRecord(joinedRecord, leftRecord, rightRecord, windowStart, windowEnd);

                        // Calling the child operator for this joinedRecord
                        child->execute(ctx, joinedRecord);

                    }//end of key compare
                }    //end of for every right key
            }        //end of for every right page
        }            //end of for every left key
    }                //end of for every left page
}

HJProbe::HJProbe(const uint64_t operatorHandlerIndex,
                 const JoinSchema& joinSchema,
                 const std::string& joinFieldNameLeft,
                 const std::string& joinFieldNameRight,
                 const WindowMetaData& windowMetaData,
                 QueryCompilation::StreamJoinStrategy joinStrategy,
                 QueryCompilation::WindowingStrategy windowingStrategy,
                 bool withDeletion)
    : StreamJoinProbe(operatorHandlerIndex,
                      joinSchema,
                      joinFieldNameLeft,
                      joinFieldNameRight,
                      windowMetaData,
                      joinStrategy,
                      windowingStrategy,
                      withDeletion),
      // As we are only ever reading a single record, we do not care about the buffer size
      leftMemProvider(
          Runtime::Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/ 1, joinSchema.leftSchema)),
      rightMemProvider(
          Runtime::Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/ 1, joinSchema.rightSchema)) {}
}// namespace NES::Runtime::Execution::Operators