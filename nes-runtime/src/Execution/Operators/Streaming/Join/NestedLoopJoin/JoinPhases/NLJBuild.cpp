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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::Operators {

void* insertEntryMemRefProxy(void* ptrOpHandler, bool isLeftSide, uint64_t timestampRecord) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");

    NLJOperatorHandler* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    return opHandler->allocateNewEntry(timestampRecord, isLeftSide);
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
    for (auto& windowIdentifier : windowIdentifiersToBeTriggered) {
        auto buffer = workerCtx->allocateTupleBuffer();
        std::memcpy(buffer.getBuffer(), &windowIdentifier, sizeof(uint64_t));
        buffer.setNumberOfTuples(1);
        pipelineCtx->dispatchBuffer(buffer);
        NES_TRACE2("Emitted windowIdentifier {}", windowIdentifier);
    }
}

void NLJBuild::execute(ExecutionContext& ctx, Record& record) const {
    // Get the global state
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // TODO for now, this is okay but we have to fix this with issue #3652
    //TODO change to timestampfunction
    Value<UInt64> timestampVal = (uint64_t) 0;
    if (timeStampField == "IngestionTime") {
        timestampVal = ctx.getCurrentTs();
    } else {
        timestampVal = record.read(timeStampField).as<UInt64>();
    }

    // Get the memRef to the new entry
    auto entryMemRef = Nautilus::FunctionCall("insertEntryMemRefProxy",
                                              insertEntryMemRefProxy,
                                              operatorHandlerMemRef,
                                              Value<Boolean>(isLeftSide),
                                              timestampVal);

    // Write Record at entryMemRef
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto& field : schema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

void NLJBuild::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // Update the watermark for the nlj operator and trigger windows
    //TODO SZ: I am not sure this is doing it at the end of the buffer which is maybe fine for now but if a particular tuple within the buffer
    // can trigger window this will be missed?
    Nautilus::FunctionCall("checkWindowsTriggerProxy",
                           checkWindowsTriggerProxyForNLJBuild,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWorkerContext(),
                           recordBuffer.getWatermarkTs(),
                           recordBuffer.getSequenceNr(),
                           recordBuffer.getOriginId());
}

NLJBuild::NLJBuild(uint64_t operatorHandlerIndex,
                   const SchemaPtr& schema,
                   const std::string& joinFieldName,
                   const std::string& timeStampField,
                   bool isLeftSide)
    : operatorHandlerIndex(operatorHandlerIndex), schema(schema), joinFieldName(joinFieldName), timeStampField(timeStampField),
      isLeftSide(isLeftSide) {}

}// namespace NES::Runtime::Execution::Operators