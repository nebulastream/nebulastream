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
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <numeric>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void setNumberOfWorkerThreadsProxy(void* ptrOpHandler, void* ptrPipelineContext) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineContext != nullptr, "pipeline context should not be null!");
    auto* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineContext);

    opHandler->setup(pipelineCtx->getNumberOfWorkerThreads());
}

void* getCurrentWindowProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);

    return opHandler->getCurrentWindowOrCreate();
}

void* getNLJWindowRefProxy(void* ptrOpHandler, uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);

    return opHandler->getWindowByTimestampOrCreateIt(timestamp).get();
}

void triggerWindowsProxy(std::vector<uint64_t>& windowIdentifiersToBeTriggered, void* ptrOpHandler, void* ptrPipelineCtx) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");

    auto* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);

    if (!windowIdentifiersToBeTriggered.empty()) {
        opHandler->triggerWindows(windowIdentifiersToBeTriggered, nullptr, pipelineCtx);
    }
}

void triggerAllWindowsProxy(void* ptrOpHandler, void* ptrPipelineCtx) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");

    auto* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    NES_DEBUG("Triggering all windows for pipelineId {}!", pipelineCtx->getPipelineID());

    auto windowIdentifiersToBeTriggered = opHandler->triggerAllWindows();
    triggerWindowsProxy(windowIdentifiersToBeTriggered, ptrOpHandler, ptrPipelineCtx);
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
    auto workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);

    //update last seen watermark by this worker
    opHandler->updateWatermarkForWorker(watermarkTs, workerCtx->getId());
    auto minWatermark = opHandler->getMinWatermarkForWorker();

    auto windowIdentifiersToBeTriggered = opHandler->checkWindowsTrigger(minWatermark, sequenceNumber, originId);
    triggerWindowsProxy(windowIdentifiersToBeTriggered, ptrOpHandler, ptrPipelineCtx);
}

void NLJBuild::updateLocalJoinState(LocalNestedLoopJoinState* localJoinState,
                                    Nautilus::Value<Nautilus::MemRef>& operatorHandlerMemRef,
                                    Nautilus::Value<Nautilus::UInt64>& timestamp,
                                    Nautilus::Value<Nautilus::UInt64>& workerId) const {
    NES_DEBUG("Updating LocalJoinState!");

    // Retrieving the window of the current watermark, as we expect that more tuples will be inserted into this window
    localJoinState->windowReference =
        Nautilus::FunctionCall("getNLJWindowRefProxy", getNLJWindowRefProxy, operatorHandlerMemRef, timestamp);
    auto nljPagedVectorMemRef = Nautilus::FunctionCall("getNLJPagedVectorProxy",
                                                       getNLJPagedVectorProxy,
                                                       localJoinState->windowReference,
                                                       workerId,
                                                       Value<UInt64>(to_underlying(joinBuildSide)));
    localJoinState->pagedVectorRef = Nautilus::Interface::PagedVectorRef(nljPagedVectorMemRef, entrySize);
    localJoinState->windowStart =
        Nautilus::FunctionCall("getNLJWindowStartProxy", getNLJWindowStartProxy, localJoinState->windowReference);
    localJoinState->windowEnd =
        Nautilus::FunctionCall("getNLJWindowEndProxy", getNLJWindowEndProxy, localJoinState->windowReference);
}

void NLJBuild::execute(ExecutionContext& ctx, Record& record) const {
    // Get the global state
    auto localJoinState = dynamic_cast<LocalNestedLoopJoinState*>(ctx.getLocalState(this));
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
    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

void NLJBuild::setup(ExecutionContext& executionCtx) const {
    Nautilus::FunctionCall("setNumberOfWorkerThreadsProxy",
                           setNumberOfWorkerThreadsProxy,
                           executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
                           executionCtx.getPipelineContext());
}

void NLJBuild::open(ExecutionContext& ctx, RecordBuffer&) const {
    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    auto workerId = ctx.getWorkerId();
    auto windowReference = Nautilus::FunctionCall("getCurrentWindowProxy", getCurrentWindowProxy, opHandlerMemRef);
    auto nljPagedVectorMemRef = Nautilus::FunctionCall("getNLJPagedVectorProxy",
                                                       getNLJPagedVectorProxy,
                                                       windowReference,
                                                       workerId,
                                                       Value<UInt64>(to_underlying(joinBuildSide)));
    auto pagedVectorRef = Nautilus::Interface::PagedVectorRef(nljPagedVectorMemRef, entrySize);
    auto localJoinState = std::make_unique<LocalNestedLoopJoinState>(opHandlerMemRef, windowReference, pagedVectorRef);

    // Getting the current window start and end
    localJoinState->windowStart =
        Nautilus::FunctionCall("getNLJWindowStartProxy", getNLJWindowStartProxy, localJoinState->windowReference);
    localJoinState->windowEnd =
        Nautilus::FunctionCall("getNLJWindowEndProxy", getNLJWindowEndProxy, localJoinState->windowReference);

    // Storing the local state
    ctx.setLocalOperatorState(this, std::move(localJoinState));
}

void NLJBuild::close(ExecutionContext& ctx, RecordBuffer&) const {
    // Update the watermark for the nlj operator and trigger windows
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("checkWindowsTriggerProxyForNLJBuild",
                           checkWindowsTriggerProxyForNLJBuild,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWorkerContext(),
                           ctx.getWatermarkTs(),
                           ctx.getSequenceNumber(),
                           ctx.getOriginId());
}

void NLJBuild::terminate(ExecutionContext& ctx) const {
    // Trigger all windows, as the query has ended
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("triggerAllWindowsProxy", triggerAllWindowsProxy, operatorHandlerMemRef, ctx.getPipelineContext());
}

NLJBuild::NLJBuild(uint64_t operatorHandlerIndex,
                   const SchemaPtr& schema,
                   const std::string& joinFieldName,
                   const QueryCompilation::JoinBuildSideType joinBuildSide,
                   TimeFunctionPtr timeFunction)
    : operatorHandlerIndex(operatorHandlerIndex), schema(schema), joinFieldName(joinFieldName), joinBuildSide(joinBuildSide),
      entrySize(schema->getSchemaSizeInBytes()), timeFunction(std::move(timeFunction)) {}

}// namespace NES::Runtime::Execution::Operators