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
#include <Execution/Operators/Streaming/Join/StreamHashJoin/JoinPhases/StreamHashJoinProbe.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Common.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cstring>

namespace NES::Runtime::Execution::Operators {

StreamHashJoinProbe::StreamHashJoinProbe(uint64_t handlerIndex,
                                         SchemaPtr joinSchemaLeft,
                                         SchemaPtr joinSchemaRight,
                                         SchemaPtr joinSchemaOutput,
                                         std::string joinFieldNameLeft,
                                         std::string joinFieldNameRight)
    : handlerIndex(handlerIndex), joinSchemaLeft(joinSchemaLeft), joinSchemaRight(joinSchemaRight),
      joinSchemaOutput(joinSchemaOutput), joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight) {}


uint64_t getWindowStartProxyForHashJoin(void* ptrJoinPartitionIdSliceId) {
    NES_ASSERT2_FMT(ptrJoinPartitionIdSliceId != nullptr, "join partition id slice id should not be null");
    auto probeInfo = static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId);
    NES_INFO("Probing for leftId {} rightId {} partitionId {} windowstart {} windowend {}", probeInfo->sliceIdentifierLeft, probeInfo->sliceIdentifierRight,
             probeInfo->partitionId, probeInfo->windowInfo.windowStart, probeInfo->windowInfo.windowEnd);

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

    if (joinBuildSide == QueryCompilation::JoinBuildSideType::Left) {
        return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->sliceIdentifierLeft;
    } else if (joinBuildSide == QueryCompilation::JoinBuildSideType::Right) {
        return static_cast<JoinPartitionIdSliceIdWindow*>(ptrJoinPartitionIdSliceId)->sliceIdentifierRight;
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

void* getHashSliceProxy(void* ptrOpHandler, uint64_t sliceIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);
    return opHandler->getSliceBySliceIdentifier(sliceIdentifier).value().get();
}

void* getTupleFromBucketAtPosProxyForHashJoin(void* hashWindowPtr,
                                              uint64_t joinBuildSideInt,
                                              uint64_t bucketPos,
                                              uint64_t pageNo,
                                              uint64_t recordPos) {
    NES_ASSERT2_FMT(hashWindowPtr != nullptr, "hashWindowPtr should not be null");
    auto hashWindow = static_cast<StreamHashJoinSlice*>(hashWindowPtr);
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    NES_INFO("Probing for {} tuples {} with start {} end {}", magic_enum::enum_name(joinBuildSide),
             hashWindow->getMergingHashTable(joinBuildSide).getNumItems(bucketPos),
             hashWindow->getSliceStart(), hashWindow->getSliceEnd());
    return hashWindow->getMergingHashTable(joinBuildSide).getTupleFromBucketAtPos(bucketPos, pageNo, recordPos);
}

uint64_t getNumberOfPagesProxyForHashJoin(void* ptrHashSlice, uint64_t joinBuildSideInt, uint64_t bucketPos) {
    NES_ASSERT2_FMT(ptrHashSlice != nullptr, "ptrHashSlice should not be null");
    const auto hashSlice = static_cast<StreamHashJoinSlice*>(ptrHashSlice);
    const auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    auto numPages =  hashSlice->getMergingHashTable(joinBuildSide).getNumPages(bucketPos);
    auto numItems =  hashSlice->getMergingHashTable(joinBuildSide).getNumItems(bucketPos);

    NES_INFO("numPages: {}, numItems {} for slide start {} end {}", numPages, numItems, hashSlice->getSliceStart(), hashSlice->getSliceEnd());
    return numPages;
}

uint64_t getNumberOfTuplesForPageProxy(void* hashWindowPtr, uint64_t joinBuildSideInt, uint64_t bucketPos, uint64_t pageNo) {
    NES_ASSERT2_FMT(hashWindowPtr != nullptr, "hashWindowPtr should not be null");
    const auto hashWindow = static_cast<StreamHashJoinSlice*>(hashWindowPtr);
    const auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    return hashWindow->getMergingHashTable(joinBuildSide).getNumberOfTuplesForPage(bucketPos, pageNo);
}

void StreamHashJoinProbe::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // As this operator functions as a scan, we have to set the execution context for this pipeline
    ctx.setWatermarkTs(recordBuffer.getWatermarkTs());
    ctx.setSequenceNumber(recordBuffer.getSequenceNr());
    ctx.setOrigin(recordBuffer.getOriginId());
    Operator::open(ctx, recordBuffer);

    // Getting all needed references or values
    const auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    const auto joinPartitionIdSliceIdMemRef = recordBuffer.getBuffer();
    const auto windowStart = Nautilus::FunctionCall("getWindowStartProxyForHashJoin", getWindowStartProxyForHashJoin,
                                                    joinPartitionIdSliceIdMemRef);
    const auto windowEnd = Nautilus::FunctionCall("getWindowEndProxyForHashJoin", getWindowEndProxyForHashJoin,
                                                  joinPartitionIdSliceIdMemRef);
    const auto partitionId = Nautilus::FunctionCall("getPartitionIdProxy", getPartitionIdProxy, joinPartitionIdSliceIdMemRef);
    const auto sliceIdLeft = Nautilus::FunctionCall("getSliceIdHJProxy", getSliceIdHJProxy, joinPartitionIdSliceIdMemRef,
                                                       Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)));
    const auto sliceIdRight = Nautilus::FunctionCall("getSliceIdHJProxy", getSliceIdHJProxy, joinPartitionIdSliceIdMemRef,
                                                        Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)));

    const auto hashSliceRefLeft = Nautilus::FunctionCall("getHashSliceProxy", getHashSliceProxy, operatorHandlerMemRef, sliceIdLeft);
    const auto hashSliceRefRight = Nautilus::FunctionCall("getHashSliceProxy", getHashSliceProxy, operatorHandlerMemRef, sliceIdRight);
    const auto numberOfPagesLeft = Nautilus::FunctionCall("getNumberOfPagesProxyForHashJoin", getNumberOfPagesProxyForHashJoin,
                                                          hashSliceRefLeft,
                                                          Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)),
                                                          partitionId);
    const auto numberOfPagesRight = Nautilus::FunctionCall("getNumberOfPagesProxyForHashJoin", getNumberOfPagesProxyForHashJoin,
                                                          hashSliceRefRight,
                                                          Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)),
                                                          partitionId);

    // TODO this can be a member that gets created during creation of this class
    const auto leftMemProvider =
        Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/ 1, joinSchemaLeft);
    const auto rightMemProvider =
        Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/ 1, joinSchemaRight);

    //TODO we should write an iterator in nautilus over the pages to make it more elegant #4067
    //for every left page
    for (Value<UInt64> leftPageNo(0_u64); leftPageNo < numberOfPagesLeft; leftPageNo = leftPageNo + 1) {
        auto numberOfTuplesLeft =
            Nautilus::FunctionCall("getNumberOfTuplesForPageProxy",
                                   getNumberOfTuplesForPageProxy,
                                   hashSliceRefLeft,
                                   Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)),
                                   partitionId,
                                   leftPageNo);
        //for every key in left page
        for (Value<UInt64> leftKeyPos(0_u64); leftKeyPos < numberOfTuplesLeft; leftKeyPos = leftKeyPos + 1) {
            auto leftRecordRef = Nautilus::FunctionCall(
                "getTupleFromBucketAtPosProxyForHashJoin",
                getTupleFromBucketAtPosProxyForHashJoin,
                hashSliceRefLeft,
                Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)),
                partitionId,
                leftPageNo,
                leftKeyPos);

            Value<UInt64> zeroValue = 0_u64;
            auto leftRecord = leftMemProvider->read({}, leftRecordRef, zeroValue);

            //TODO we should write an iterator in nautilus over the pages to make it more elegant #4067
            //for every right page
            for (Value<UInt64> rightPageNo((uint64_t) 0); rightPageNo < numberOfPagesRight; rightPageNo = rightPageNo + 1) {
                auto numberOfTuplesRight = Nautilus::FunctionCall(
                    "getNumberOfTuplesForPageProxy",
                    getNumberOfTuplesForPageProxy,
                    hashSliceRefRight,
                    Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)),
                    partitionId,
                    rightPageNo);

                //TODO: introduce Bloomfilter here #3909
                //                if (!rhsPage->bloomFilterCheck(lhsKeyPtr, sizeOfLeftKey)) {
                //                    continue;
                //                }

                //for every key in right page
                for (Value<UInt64> rightKeyPos((uint64_t) 0); rightKeyPos < numberOfTuplesRight; rightKeyPos = rightKeyPos + 1) {
                    auto rightRecordRef = Nautilus::FunctionCall(
                        "getTupleFromBucketAtPosProxyForHashJoin",
                        getTupleFromBucketAtPosProxyForHashJoin,
                        hashSliceRefRight,
                        Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)),
                        partitionId,
                        rightPageNo,
                        rightKeyPos);

                    auto rightRecord = rightMemProvider->read({}, rightRecordRef, zeroValue);
                    if (leftRecord.read(joinFieldNameLeft) == rightRecord.read(joinFieldNameRight)) {
                        Record joinedRecord;

                        // TODO replace this with a more useful version, similar to NLJProbe, maybe create an issue for this
                        joinedRecord.write(joinSchemaOutput->get(0)->getName(), windowStart);
                        joinedRecord.write(joinSchemaOutput->get(1)->getName(), windowEnd);
                        joinedRecord.write(joinSchemaOutput->get(2)->getName(), leftRecord.read(joinFieldNameLeft));

                        /* Writing the leftSchema fields, expect the join schema to have the fields in the same order then
                         * the left schema */
                        for (auto& field : joinSchemaLeft->fields) {
                            joinedRecord.write(field->getName(), leftRecord.read(field->getName()));
                        }

                        /* Writing the rightSchema fields, expect the join schema to have the fields in the same order then
                         * the right schema */
                        for (auto& field : joinSchemaRight->fields) {
                            joinedRecord.write(field->getName(), rightRecord.read(field->getName()));
                        }

                        // Calling the child operator for this joinedRecord
                        child->execute(ctx, joinedRecord);

                    }//end of key compare
                }    //end of for every right key
            }        //end of for every right page
        }            //end of for every left key
    }                //end of for every left page
}

void StreamHashJoinProbe::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    if (withDeletion) {
        // Update the watermark for the nlj probe and delete all slices that can be deleted
        const auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
        Nautilus::FunctionCall("deleteAllSlicesProxy",
                               deleteAllSlicesProxy,
                               operatorHandlerMemRef,
                               ctx.getWatermarkTs(),
                               ctx.getSequenceNumber(),
                               ctx.getOriginId());
    }

    // Now close for all children
    Operator::close(ctx, recordBuffer);
}

}//namespace NES::Runtime::Execution::Operators