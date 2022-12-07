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

#include <Nautilus/Interface/FunctionCall.hpp>
#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinSink.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <cstring>
#include <Execution/Operators/ExecutableOperator.hpp>


namespace NES::Runtime::Execution::Operators {

LazyJoinSink::LazyJoinSink(uint64_t handlerIndex)
    : handlerIndex(handlerIndex) {}



bool compareField(uint8_t* fieldPtr1, uint8_t * fieldPtr2, size_t sizeOfField) {
    return memcmp(fieldPtr1, fieldPtr2, sizeOfField) == 0;
}

uint8_t* getField(uint8_t* recordBase, SchemaPtr joinSchema, const std::string& fieldName) {
    uint8_t* pointer = recordBase;
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto& field : joinSchema->fields) {
        if (field->getName() == fieldName) {
            break;
        }
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        pointer += fieldType->size();
    }
    return pointer;
}

size_t getSizeOfKey(SchemaPtr joinSchema, const std::string& joinFieldName) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto const keyType = physicalDataTypeFactory.getPhysicalType(joinSchema->get(joinFieldName)->getDataType());
    return keyType->size();
}

size_t executeJoin(PipelineExecutionContext* pipelineCtx, WorkerContext* workerCtx, LazyJoinOperatorHandler* operatorHandler,
                   std::vector<FixedPage>&& probeSide,
                   std::vector<FixedPage>&& buildSide) {

    size_t joinedTuples = 0;

    size_t sizeOfKey = getSizeOfKey(operatorHandler->getJoinSchemaLeft(), operatorHandler->getJoinFieldNameLeft());

    for(auto& lhsPage : probeSide) {
        auto lhsLen = lhsPage.size();
        for (auto i = 0UL; i < lhsLen; ++i) {
            auto lhsRecordPtr = lhsPage[i];
            auto lhsKeyPtr =
                getField(lhsRecordPtr, operatorHandler->getJoinSchemaLeft(), operatorHandler->getJoinFieldNameLeft());

            for(auto& rhsPage : buildSide) {
                auto rhsLen = rhsPage.size();
                if (rhsLen == 0 || !rhsPage.bloomFilterCheck(lhsKeyPtr, sizeOfKey)) {
                    continue;
                }

                for (auto j = 0UL; j < rhsLen; ++j) {
                    auto rhsRecordPtr = rhsPage[j];
                    auto rhsRecordKeyPtr =
                        getField(rhsRecordPtr, operatorHandler->getJoinSchemaRight(), operatorHandler->getJoinFieldNameRight());
                    if (compareField(lhsKeyPtr, rhsRecordKeyPtr , sizeOfKey)) {
                        ++joinedTuples;

                        // TODO ask Philipp how can I set win1win2$start and win1win2$end
                        // TODO ask Philipp if I should support columnar layout as the current implementation does not support it


                        auto buffer = workerCtx->allocateTupleBuffer();
                        auto bufferPtr = buffer.getBuffer<uint8_t>();
                        auto leftSchemaSize = operatorHandler->getJoinSchemaLeft()->getSchemaSizeInBytes();
                        auto rightSchemaSize = operatorHandler->getJoinSchemaRight()->getSchemaSizeInBytes();

                        memcpy(bufferPtr, lhsKeyPtr, sizeOfKey);
                        memcpy(bufferPtr + sizeOfKey, lhsRecordPtr, leftSchemaSize);
                        memcpy(bufferPtr + sizeOfKey + leftSchemaSize, rhsRecordPtr, rightSchemaSize);

                        pipelineCtx->emitBuffer(buffer, reinterpret_cast<WorkerContext&>(workerCtx));
                    }
                }
            }

        }
    }


    return joinedTuples;
}


void performJoin(void* ptrOpHandler, void* ptrPipelineCtx, void* ptrWorkerCtx, void* joinPartitionTimeStampPtr) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");
    NES_ASSERT2_FMT(joinPartitionTimeStampPtr != nullptr, "joinPartitionTimeStampPtr should not be null");


    auto opHandler = static_cast<LazyJoinOperatorHandler*>(ptrOpHandler);
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    auto workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);
    auto joinPartTimestamp = static_cast<JoinPartitionIdTumpleStamp*>(joinPartitionTimeStampPtr);

    const auto partitionId = joinPartTimestamp->partitionId;
    const auto lastTupleTimeStamp = joinPartTimestamp->lastTupleTimeStamp;


    auto& leftHashTable = opHandler->getWindow(lastTupleTimeStamp).getSharedJoinHashTable(true /* isLeftSide */);
    auto& rightHashTable = opHandler->getWindow(lastTupleTimeStamp).getSharedJoinHashTable(false /* isLeftSide */);

    auto leftBucket = leftHashTable.getPagesForBucket(partitionId);
    auto rightBucket = rightHashTable.getPagesForBucket(partitionId);
    auto leftBucketSize = leftHashTable.getNumItems(partitionId);
    auto rightBucketSize = rightHashTable.getNumItems(partitionId);

    size_t joinedTuples = 0;
    if (leftBucketSize && rightBucketSize) {
        if (leftBucketSize > rightBucketSize) {
            joinedTuples = executeJoin(pipelineCtx, workerCtx, opHandler, std::move(rightBucket), std::move(leftBucket));
        } else {
            joinedTuples = executeJoin(pipelineCtx, workerCtx, opHandler, std::move(leftBucket), std::move(rightBucket));
        }
    }

    if (joinedTuples) {
        NES_DEBUG("Worker " << workerCtx->getId() << " got " << partitionId << " joined #tuple=" << joinedTuples);
        NES_ASSERT2_FMT(joinedTuples <= (leftBucketSize * rightBucketSize),
                        "Something wrong #joinedTuples= " << joinedTuples << " upper bound "
                                                          << (leftBucketSize * rightBucketSize));
    }


    if (opHandler->getWindow(joinPartTimestamp->lastTupleTimeStamp).fetchSubSink(1) == 1) {
        // delete the current hash table
        opHandler->deleteWindow(joinPartTimestamp->lastTupleTimeStamp);
    }


}

void LazyJoinSink::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const{
//    auto partitionId = recordBuffer.getBuffer().load<UInt64>() - 1;
//    auto sizeOfPartitionId = Int8(sizeof(size_t));
//    auto timeStampPtr = recordBuffer.getBuffer()->add(sizeOfPartitionId);
//    auto timeStamp = timeStampPtr->load<UInt64>()->getValue();

    auto joinPartitionTimestampPtr = recordBuffer.getBuffer();

    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);

    Nautilus::FunctionCall("performJoin", performJoin, operatorHandlerMemRef, ctx.getPipelineContext(), ctx.getWorkerContext(),
                           joinPartitionTimestampPtr);


}


} //namespace NES::Runtime::Execution::Operators