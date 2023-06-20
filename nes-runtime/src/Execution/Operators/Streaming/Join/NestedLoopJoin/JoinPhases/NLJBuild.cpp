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
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>

namespace NES::Runtime::Execution::Operators {

void* getNLJWindowRefProxy(void* ptrOpHandler, uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);

    return opHandler->getWindowByTimestampOrCreateIt2(timestamp);
}

uint64_t getNLJWindowStartProxy(void* ptrNljWindow) {
    NES_ASSERT2_FMT(ptrNljWindow != nullptr, "nlj window pointer should not be null!");
    auto* nljWindow = static_cast<NLJWindow*>(ptrNljWindow);
    return nljWindow->getWindowStart();
}

uint64_t getNLJWindowEndProxy(void* ptrNljWindow) {
    NES_ASSERT2_FMT(ptrNljWindow != nullptr, "nlj window pointer should not be null!");
    auto* nljWindow = static_cast<NLJWindow*>(ptrNljWindow);
    return nljWindow->getWindowEnd();
}

/**
 * @brief Updates the windowState of all windows and emits buffers, if the windows can be emitted
 */
void checkWindowsTriggerProxyForNLJBuild(void* ptrOpHandler,
                                         void* ptrPipelineCtx,
                                         void* ptrWorkerCtx,
                                         uint64_t watermarkTs,
                                         uint64_t sequenceNumber,
                                         OriginId originId) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");

    auto* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    auto workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);

    //TODO: Multi threaded this can become a problem
    auto windowIdentifiersToBeTriggered = opHandler->checkWindowsTrigger(watermarkTs, sequenceNumber, originId);
    if (windowIdentifiersToBeTriggered.size() > 0) {
        opHandler->triggerWindows(windowIdentifiersToBeTriggered, workerCtx, pipelineCtx);
    }
}

void NLJBuild::updateLocalJoinState(LocalNestedLoopJoinState* localJoinState,
                                    Nautilus::Value<Nautilus::MemRef>& operatorHandlerMemRef,
                                    Nautilus::Value<Nautilus::UInt64>& timestamp,
                                    Nautilus::Value<Nautilus::UInt64>& workerId) const {
    // Retrieving the window of the current watermark, as we expect that more tuples will be inserted into this window
    localJoinState->windowReference = Nautilus::FunctionCall("getNLJWindowRefProxy", getNLJWindowRefProxy,
                                                             operatorHandlerMemRef, timestamp);
    auto nljPagedVectorMemRef = Nautilus::FunctionCall("getNLJPagedVectorProxy", getNLJPagedVectorProxy,
                                                       localJoinState->windowReference, workerId,
                                                       Nautilus::Value<Nautilus::Boolean>(isLeftSide));
    Nautilus::Value<UInt64> entrySize = Nautilus::FunctionCall("getEntrySizePagedVector", getEntrySizePagedVector,
                                                                    nljPagedVectorMemRef);
    Nautilus::Value<UInt64> pageSize = Nautilus::FunctionCall("getPageSizePagedVector", getPageSizePagedVector,
                                                                  nljPagedVectorMemRef);
    localJoinState->pagedVectorRef = Nautilus::Interface::PagedVectorRef(nljPagedVectorMemRef,
                                                                         entrySize.getValue().getValue(),
                                                                         pageSize.getValue().getValue());

    localJoinState->windowStart = Nautilus::FunctionCall("getNLJWindowStartProxy", getNLJWindowStartProxy,
                                                         localJoinState->windowReference);
    localJoinState->windowEnd = Nautilus::FunctionCall("getNLJWindowEndProxy", getNLJWindowEndProxy,
                                                       localJoinState->windowReference);
}

void NLJBuild::execute(ExecutionContext& ctx, Record& record) const {
    // Get the global state
    auto* localJoinState = static_cast<LocalNestedLoopJoinState*>(ctx.getLocalState(this));
    auto operatorHandlerMemRef = localJoinState->joinOperatorHandler;
    Value<UInt64> timestampVal = timeFunction->getTs(ctx, record);

    if (!(localJoinState->windowStart <= timestampVal && timestampVal < localJoinState->windowEnd)) {
        // We have to get the window for the current timestamp
        auto workerId = ctx.getWorkerId();
        updateLocalJoinState(localJoinState, operatorHandlerMemRef, timestampVal, workerId);
    }

    // Get the memRef to the new entry
    auto entryMemRef = localJoinState->pagedVectorRef.allocateEntry();


    // Write Record at entryMemRef
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto& field : schema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

void NLJBuild::open(ExecutionContext &ctx, RecordBuffer&) const {
    auto opHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto windowRef = Nautilus::Value<Nautilus::MemRef>(nullptr);
    auto pagedVectorRef = Nautilus::Interface::PagedVectorRef(Nautilus::Value<Nautilus::MemRef>(nullptr), 42);

    auto localJoinState = std::make_unique<LocalNestedLoopJoinState>(opHandler, windowRef, pagedVectorRef);
    ctx.setLocalOperatorState(this, std::move(localJoinState));
}

void NLJBuild::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    auto* localJoinState = static_cast<LocalNestedLoopJoinState*>(ctx.getLocalState(this));
    auto operatorHandlerMemRef = localJoinState->joinOperatorHandler;

    auto watermarkTs = recordBuffer.getWatermarkTs();
    if (!(localJoinState->windowStart <= watermarkTs && watermarkTs < localJoinState->windowEnd)) {
        auto workerId = ctx.getWorkerId();
        updateLocalJoinState(localJoinState, operatorHandlerMemRef, watermarkTs, workerId);
    }

    ctx.setWatermarkTs(watermarkTs);
    ctx.setOrigin(recordBuffer.getOriginId());

    // Update the watermark for the nlj operator and trigger windows
    Nautilus::FunctionCall("checkWindowsTriggerProxyForNLJBuild",
                           checkWindowsTriggerProxyForNLJBuild,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWorkerContext(),
                           recordBuffer.getWatermarkTs(),
                           recordBuffer.getSequenceNr(),
                           recordBuffer.getOriginId());
}

NLJBuild::NLJBuild(uint64_t operatorHandlerIndex,
                   const SchemaPtr schema,
                   const std::string& joinFieldName,
                   const std::string& timeStampField,
                   bool isLeftSide,
                   std::shared_ptr<TimeFunction> timeFunction)
    : operatorHandlerIndex(operatorHandlerIndex), schema(schema), joinFieldName(joinFieldName), timeStampField(timeStampField),
      isLeftSide(isLeftSide), timeFunction(std::move(timeFunction)) {}

}// namespace NES::Runtime::Execution::Operators