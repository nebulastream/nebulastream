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

#include <atomic>
#include <cstdint>

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/JoinPhases/StreamHashJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::Operators {

void* getLocalHashTableFunctionCall(void* ptrOpHandler, size_t index, bool isLeftSide, uint64_t timeStamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    StreamHashJoinOperatorHandler* opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);
    auto currentWindow = opHandler->getWindowByTimestamp(timeStamp);
    //TODO I am not sure if while is a good idea here or if it is just an error
    while (!currentWindow.has_value()) {
        opHandler->createNewWindow(timeStamp);
        currentWindow = opHandler->getWindowByTimestamp(timeStamp);
    }
    StreamHashJoinWindow* hashWindow = static_cast<StreamHashJoinWindow*>(currentWindow->get());
    NES_TRACE("Insert into HT for window=" << hashWindow->getWindowIdentifier() << " is left=" << isLeftSide << " idx=" << index);
    auto localHashTablePointer = static_cast<void*>(hashWindow->getLocalHashTable(index, isLeftSide));
    return localHashTablePointer;
}

void* insertFunctionCall(void* ptrLocalHashTable, uint64_t key) {
    NES_ASSERT2_FMT(ptrLocalHashTable != nullptr, "ptrLocalHashTable should not be null");
    LocalHashTable* localHashTable = static_cast<LocalHashTable*>(ptrLocalHashTable);
    return localHashTable->insert(key);
}

/**
 * @brief Updates the windowState of all windows and emits buffers, if the windows can be emitted
 */
void checkWindowsTriggerProxyForJoinBuild(void* ptrOpHandler,
                                          void* ptrPipelineCtx,
                                          void* ptrWorkerCtx,
                                          uint64_t watermarkTs,
                                          uint64_t sequenceNumber,
                                          OriginId originId,
                                          bool isLeftSide) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");

    auto* opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    auto workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);

    //update last seen watermark by this worker
    opHandler->updateWatermarkForWorker(watermarkTs, workerCtx->getId());
    auto minWatermark = opHandler->getMinWatermarkForWorker();

    auto windowIdentifiersToBeTriggered = opHandler->checkWindowsTrigger(minWatermark, sequenceNumber, originId);

    //one of the two builds identify that we now need to trigger
    //TODO: check if this is really threadsafe

    //for every window
    for (auto& windowIdentifier : windowIdentifiersToBeTriggered) {
        //for every partition within the window create one task
        //get current window
        auto currentWindow = opHandler->getWindowByWindowIdentifier(windowIdentifier);
        NES_ASSERT(currentWindow.has_value(), "Triggering window does not exist for ts=" << windowIdentifier);
        auto hashWindow = static_cast<StreamHashJoinWindow*>(currentWindow->get());
        auto& sharedJoinHashTableLeft = hashWindow->getSharedJoinHashTable(true);
        auto& sharedJoinHashTableRight = hashWindow->getSharedJoinHashTable(false);

        NES_DEBUG("Trigger window =" << hashWindow->getWindowIdentifier() << " from leftSide=" << isLeftSide);
        for (auto i = 0UL; i < opHandler->getNumPartitions(); ++i) {
            //push actual bucket from local to global hash table for left side
            auto localHashTableLeft = hashWindow->getLocalHashTable(workerCtx->getId(), true);
            sharedJoinHashTableLeft.insertBucket(i, localHashTableLeft->getBucketLinkedList(i));

            //push actual bucket from local to global hash table for right side
            auto localHashTableRight = hashWindow->getLocalHashTable(workerCtx->getId(), false);
            sharedJoinHashTableRight.insertBucket(i, localHashTableRight->getBucketLinkedList(i));

            //create task for current window and current partition
            auto buffer = workerCtx->allocateTupleBuffer();
            auto bufferAs = buffer.getBuffer<JoinPartitionIdTWindowIdentifier>();
            bufferAs->partitionId = i;
            bufferAs->windowIdentifier = windowIdentifier;
            buffer.setNumberOfTuples(1);
            pipelineCtx->dispatchBuffer(buffer);
            NES_TRACE2("Emitted windowIdentifier {}", windowIdentifier);
        }
    }
}

void setupOperatorHandler(void* ptrOpHandler, void* ptrPipelineCtx) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");

    auto opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);

    opHandler->setup(pipelineCtx->getNumberOfWorkerThreads());
}

void StreamHashJoinBuild::execute(ExecutionContext& ctx, Record& record) const {
    // Get the global state
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);

    Value<UInt64> tsValue = (uint64_t) 0;
    if (timeStampField == "IngestionTime") {
        tsValue = ctx.getCurrentTs();
    } else {
        tsValue = record.read(timeStampField).as<UInt64>();
    }

    auto localHashTableMemRef = Nautilus::FunctionCall("getLocalHashTableFunctionCall",
                                                       getLocalHashTableFunctionCall,
                                                       operatorHandlerMemRef,
                                                       ctx.getWorkerId(),
                                                       Value<Boolean>(isLeftSide),
                                                       Value<UInt64>(tsValue));

    //get position in the HT where to write to
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto entryMemRef = Nautilus::FunctionCall("insertFunctionCall",
                                              insertFunctionCall,
                                              localHashTableMemRef,
                                              record.read(joinFieldName).as<UInt64>());
    //write data
    for (auto& field : schema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

void StreamHashJoinBuild::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);

    // Update the watermark for the nlj operator and trigger windows
    //TODO SZ: I am not sure this is doing it at the end of the buffer which is maybe fine for now but if a particular tuple within the buffer
    // can trigger window this will be missed?
    Nautilus::FunctionCall("checkWindowsTriggerProxy",
                           checkWindowsTriggerProxyForJoinBuild,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWorkerContext(),
                           recordBuffer.getWatermarkTs(),
                           recordBuffer.getSequenceNr(),
                           recordBuffer.getOriginId(),
                           Value<Boolean>(isLeftSide));
}

void StreamHashJoinBuild::setup(ExecutionContext& ctx) const {
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    Nautilus::FunctionCall("setupOperatorHandler", setupOperatorHandler, operatorHandlerMemRef, ctx.getPipelineContext());
}

StreamHashJoinBuild::StreamHashJoinBuild(uint64_t handlerIndex,
                                         bool isLeftSide,
                                         const std::string& joinFieldName,
                                         const std::string& timeStampField,
                                         SchemaPtr schema)
    : handlerIndex(handlerIndex), isLeftSide(isLeftSide), joinFieldName(joinFieldName), timeStampField(timeStampField),
      schema(schema) {}

}// namespace NES::Runtime::Execution::Operators