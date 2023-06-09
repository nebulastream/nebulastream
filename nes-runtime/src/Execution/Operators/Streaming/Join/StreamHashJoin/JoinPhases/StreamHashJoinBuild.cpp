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
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/JoinPhases/StreamHashJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
namespace NES::Runtime::Execution::Operators {

uint64_t lastTimeStamp = std::numeric_limits<uint64_t>::max();
StreamHashJoinWindow* lastHashWindow;

void* getStreamHashJoinWindowProxy(void* ptrOpHandler, uint64_t timeStamp) {
    if (timeStamp == lastTimeStamp) {
        return static_cast<void*>(lastHashWindow);
    } else {
        NES_DEBUG2("getStreamHashJoinWindowProxy with ts={}", timeStamp);
        StreamHashJoinOperatorHandler* opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);
        auto currentWindow = opHandler->getWindowByTimestampOrCreateIt(timeStamp);
        StreamHashJoinWindow* hashWindow = static_cast<StreamHashJoinWindow*>(currentWindow.get());

        lastTimeStamp = timeStamp;
        lastHashWindow = hashWindow;
        return static_cast<void*>(hashWindow);
    }
}

void* getLocalHashTableProxy(void* ptrHashWindow, size_t workerIdx, bool isLeftSide) {
    NES_ASSERT2_FMT(ptrHashWindow != nullptr, "hash window handler context should not be null");
    StreamHashJoinWindow* hashWindow = static_cast<StreamHashJoinWindow*>(ptrHashWindow);
    NES_DEBUG2("Insert into HT for window={} is left={} workerIdx={}", hashWindow->getWindowIdentifier(), isLeftSide, workerIdx);
    auto localHashTablePointer = static_cast<void*>(hashWindow->getLocalHashTable(workerIdx, isLeftSide));
    return localHashTablePointer;
}

void* insertFunctionProxy(void* ptrLocalHashTable, uint64_t key) {
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
                                          OriginId originId) {
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

    if (windowIdentifiersToBeTriggered.size() > 0) {
        opHandler->triggerWindows(windowIdentifiersToBeTriggered, workerCtx, pipelineCtx);
    }
}

void StreamHashJoinBuild::execute(ExecutionContext& ctx, Record& record) const {
    //void StreamHashJoinBuild::execute(ExecutionContext& ctx, Record& record) const {
    // Get the global state
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);

    Value<UInt64> tsValue = timeFunction->getTs(ctx, record);
    //
    auto localHashWindowRef = Nautilus::FunctionCall("getStreamHashJoinWindowProxy",
                                                     getStreamHashJoinWindowProxy,
                                                     operatorHandlerMemRef,
                                                     Value<UInt64>(tsValue));
    //
    //    auto localHashTableMemRef = Nautilus::FunctionCall("getLocalHashTableProxy",
    //                                                       getLocalHashTableProxy,
    //                                                       localHashWindowRef,
    //                                                       ctx.getWorkerId(),
    //                                                       Value<Boolean>(isLeftSide));

    //    //get position in the HT where to write to
    //    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    //    auto entryMemRef = Nautilus::FunctionCall("insertFunctionProxy",
    //                                              insertFunctionProxy,
    //                                              localHashTableMemRef,
    //                                              record.read(joinFieldName).as<UInt64>());
    //    //write data
    //    for (auto& field : schema->fields) {
    //        auto const fieldName = field->getName();
    //        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
    //
    //        entryMemRef.store(record.read(fieldName));
    //        entryMemRef = entryMemRef + fieldType->size();
    //    }
}

void StreamHashJoinBuild::close(ExecutionContext& ctx, RecordBuffer&) const {
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    NES_DEBUG(isLeftSide);
    // Update the watermark and trigger windows
    Nautilus::FunctionCall("checkWindowsTriggerProxy",
                           checkWindowsTriggerProxyForJoinBuild,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWorkerContext(),
                           ctx.getWatermarkTs(),
                           ctx.getSequenceNumber(),
                           ctx.getOriginId());
}

void setupOperatorHandler(void* ptrOpHandler, void* ptrPipelineCtx) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");

    auto opHandler = static_cast<StreamHashJoinOperatorHandler*>(ptrOpHandler);
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);

    opHandler->setup(pipelineCtx->getNumberOfWorkerThreads());
}

void StreamHashJoinBuild::setup(ExecutionContext& ctx) const {
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    Nautilus::FunctionCall("setupOperatorHandler", setupOperatorHandler, operatorHandlerMemRef, ctx.getPipelineContext());
}

StreamHashJoinBuild::StreamHashJoinBuild(uint64_t handlerIndex,
                                         bool isLeftSide,
                                         const std::string& joinFieldName,
                                         const std::string& timeStampField,
                                         SchemaPtr schema,
                                         std::shared_ptr<TimeFunction> timeFunction)
    : handlerIndex(handlerIndex), isLeftSide(isLeftSide), joinFieldName(joinFieldName), timeStampField(timeStampField),
      schema(schema), timeFunction(std::move(timeFunction)) {}

}// namespace NES::Runtime::Execution::Operators