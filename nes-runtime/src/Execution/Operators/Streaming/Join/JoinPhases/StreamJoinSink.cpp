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
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/JoinPhases/StreamJoinSink.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <cstring>

namespace NES::Runtime::Execution::Operators {

StreamJoinSink::StreamJoinSink(uint64_t handlerIndex) : handlerIndex(handlerIndex) {}

/**
 * @brief Checks if two fields are similar
 * @param fieldPtr1
 * @param fieldPtr2
 * @param sizeOfField
 * @return true if both fields are equal, false otherwise
 */
bool compareField(uint8_t* fieldPtr1, uint8_t* fieldPtr2, size_t sizeOfField) {
    return memcmp(fieldPtr1, fieldPtr2, sizeOfField) == 0;
}

/**
 * @brief Returns a pointer to the field of the record (recordBase)
 * @param recordBase
 * @param joinSchema
 * @param fieldName
 * @return pointer to the field
 */
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

/**
 * @brief Returns the size of the key in Bytes
 * @param joinSchema
 * @param joinFieldName
 * @return size of the key in Bytes
 */
size_t getSizeOfKey(SchemaPtr joinSchema, const std::string& joinFieldName) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto const keyType = physicalDataTypeFactory.getPhysicalType(joinSchema->get(joinFieldName)->getDataType());
    return keyType->size();
}

/**
 * @brief Performs the join of two buckets probeSide and buildSide
 * @param pipelineCtx
 * @param workerCtx
 * @param operatorHandler
 * @param probeSide
 * @param buildSide
 * @return number of joined tuples
 */
size_t executeJoinForBuckets(PipelineExecutionContext* pipelineCtx,
                             WorkerContext* workerCtx,
                             StreamJoinOperatorHandler* operatorHandler,
                             std::vector<FixedPage>&& probeSide,
                             std::vector<FixedPage>&& buildSide) {

    auto joinSchema = Util::createJoinSchema(operatorHandler->getJoinSchemaLeft(),
                                             operatorHandler->getJoinSchemaRight(),
                                             operatorHandler->getJoinFieldNameLeft());
    const size_t sizeOfKey = getSizeOfKey(operatorHandler->getJoinSchemaLeft(), operatorHandler->getJoinFieldNameLeft());

    auto sizeOfJoinedTuple = joinSchema->getSchemaSizeInBytes();
    auto tuplePerBuffer = pipelineCtx->getBufferManager()->getBufferSize() / sizeOfJoinedTuple;

    auto currentTupleBuffer = workerCtx->allocateTupleBuffer();
    currentTupleBuffer.setNumberOfTuples(0);
    size_t joinedTuples = 0;
    const auto leftSchemaSize = operatorHandler->getJoinSchemaLeft()->getSchemaSizeInBytes();
    const auto rightSchemaSize = operatorHandler->getJoinSchemaRight()->getSchemaSizeInBytes();

    for (auto& lhsPage : probeSide) {
        auto lhsLen = lhsPage.size();
        for (auto i = 0UL; i < lhsLen; ++i) {
            auto lhsRecordPtr = lhsPage[i];
            auto lhsKeyPtr =
                getField(lhsRecordPtr, operatorHandler->getJoinSchemaLeft(), operatorHandler->getJoinFieldNameLeft());

            for (auto& rhsPage : buildSide) {
                auto rhsLen = rhsPage.size();

                // Checking if the key is on the page with the bloom filter
                if (rhsLen == 0 || !rhsPage.bloomFilterCheck(lhsKeyPtr, sizeOfKey)) {
                    continue;
                }

                // Iterating through all tuples of the page as we do not know where the exact tuple is
                for (auto j = 0UL; j < rhsLen; ++j) {
                    auto rhsRecordPtr = rhsPage[j];
                    auto rhsRecordKeyPtr =
                        getField(rhsRecordPtr, operatorHandler->getJoinSchemaRight(), operatorHandler->getJoinFieldNameRight());
                    if (compareField(lhsKeyPtr, rhsRecordKeyPtr, sizeOfKey)) {
                        ++joinedTuples;

                        auto numberOfTuplesInBuffer = currentTupleBuffer.getNumberOfTuples();
                        auto bufferPtr = currentTupleBuffer.getBuffer() + sizeOfJoinedTuple * numberOfTuplesInBuffer;

                        // Building the join tuple ( key | left tuple | right tuple)
                        memcpy(bufferPtr, lhsKeyPtr, sizeOfKey);
                        memcpy(bufferPtr + sizeOfKey, lhsRecordPtr, leftSchemaSize);
                        memcpy(bufferPtr + sizeOfKey + leftSchemaSize, rhsRecordPtr, rightSchemaSize);

                        numberOfTuplesInBuffer += 1;
                        currentTupleBuffer.setNumberOfTuples(numberOfTuplesInBuffer);

                        // If the buffer is full, then emitting the current one and allocating a new one
                        if (numberOfTuplesInBuffer >= tuplePerBuffer) {
                            pipelineCtx->emitBuffer(currentTupleBuffer, reinterpret_cast<WorkerContext&>(workerCtx));

                            currentTupleBuffer = workerCtx->allocateTupleBuffer();
                            currentTupleBuffer.setNumberOfTuples(0);
                        }
                    }
                }
            }
        }
    }

    // If in the current buffer are any tuples, we have to emit then before returning
    if (currentTupleBuffer.getNumberOfTuples() > 0) {
        pipelineCtx->emitBuffer(currentTupleBuffer, reinterpret_cast<WorkerContext&>(workerCtx));
    }

    return joinedTuples;
}

void performJoin(void* ptrOpHandler, void* ptrPipelineCtx, void* ptrWorkerCtx, void* joinPartitionTimeStampPtr) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");
    NES_ASSERT2_FMT(joinPartitionTimeStampPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

    auto opHandler = static_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    auto workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);
    auto joinPartTimestamp = static_cast<JoinPartitionIdTumpleStamp*>(joinPartitionTimeStampPtr);

    const auto partitionId = joinPartTimestamp->partitionId;
    const auto lastTupleTimeStamp = joinPartTimestamp->lastTupleTimeStamp;

    NES_TRACE("Joining for partition " << partitionId << " and lastTupleTimeStamp " << lastTupleTimeStamp);

    auto& leftHashTable = opHandler->getWindow(lastTupleTimeStamp).getSharedJoinHashTable(true /* isLeftSide */);
    auto& rightHashTable = opHandler->getWindow(lastTupleTimeStamp).getSharedJoinHashTable(false /* isLeftSide */);

    auto leftBucket = leftHashTable.getPagesForBucket(partitionId);
    auto rightBucket = rightHashTable.getPagesForBucket(partitionId);
    auto leftBucketSize = leftHashTable.getNumItems(partitionId);
    auto rightBucketSize = rightHashTable.getNumItems(partitionId);

    size_t joinedTuples = 0;
    if (leftBucketSize && rightBucketSize) {
        if (leftBucketSize > rightBucketSize) {
            joinedTuples =
                executeJoinForBuckets(pipelineCtx, workerCtx, opHandler, std::move(rightBucket), std::move(leftBucket));
        } else {
            joinedTuples =
                executeJoinForBuckets(pipelineCtx, workerCtx, opHandler, std::move(leftBucket), std::move(rightBucket));
        }
    }

    if (joinedTuples > 0) {
        NES_TRACE("Worker " << workerCtx->getId() << " got partitionId " << partitionId << " joined #tuple=" << joinedTuples);
        NES_ASSERT2_FMT(joinedTuples <= (leftBucketSize * rightBucketSize),
                        "Something wrong #joinedTuples= " << joinedTuples << " upper bound "
                                                          << (leftBucketSize * rightBucketSize));
    }

    if (opHandler->getWindow(lastTupleTimeStamp).fetchSubSink(1) == 1) {
        // delete the current hash table
        opHandler->deleteWindow(lastTupleTimeStamp);
    }
}

void StreamJoinSink::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {

    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    auto joinPartitionTimestampPtr = recordBuffer.getBuffer();

    Nautilus::FunctionCall("performJoin",
                           performJoin,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWorkerContext(),
                           joinPartitionTimestampPtr);
}

}//namespace NES::Runtime::Execution::Operators