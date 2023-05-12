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
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Stack/StackRef.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>



namespace NES::Runtime::Execution::Operators {

void* insertEntryMemRefProxy(void* ptrOpHandler, bool isLeftSide, uint64_t timestampRecord) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");

    NLJOperatorHandler* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    return opHandler->insertNewTuple(timestampRecord, isLeftSide);
}

bool updateStateOfNLJWindows(void* ptrOpHandler, uint64_t timestampRecord, bool isLeftSide) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");

    auto* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    return opHandler->updateStateOfNLJWindows(timestampRecord, isLeftSide);
}

void triggerJoinSinkProxy(void* ptrOpHandler, void* ptrPipelineCtx, void* ptrWorkerCtx) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");

    auto* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    auto workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);

    auto& allNLJWindows = opHandler->getAllNLJWindows();
    for (auto& nljWindow : allNLJWindows) {
        auto expected = NLJWindow::WindowState::DONE_FILLING;
        if (nljWindow.compareExchangeStrong(expected, NLJWindow::WindowState::EMITTED_TO_NLJ_SINK)) {
            auto buffer = workerCtx->allocateTupleBuffer();
            auto windowEnd = nljWindow.getWindowEnd();
            std::memcpy(buffer.getBuffer(), &windowEnd, sizeof(uint64_t));
            buffer.setNumberOfTuples(1);
            pipelineCtx->emitBuffer(buffer, *workerCtx);
            NES_TRACE2("Emitted window {}", nljWindow.toString());
        }
    }
}

/**
 * @brief Updates the windowState of all windows and emits buffers, if the windows can be emitted
 */
void checkWindowsTriggerProxy(void* ptrOpHandler, void* ptrPipelineCtx, void* ptrWorkerCtx, bool isLeftSide) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");

    auto* opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    auto workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);

    auto windowIdentifiersToBeTriggered = opHandler->checkWindowsTrigger();
    for (auto& windowIdentifier : windowIdentifiersToBeTriggered) {
        auto buffer = workerCtx->allocateTupleBuffer();
        std::memcpy(buffer.getBuffer(), &windowIdentifier, sizeof(uint64_t));
        buffer.setNumberOfTuples(1);
        pipelineCtx->emitBuffer(buffer, *workerCtx);
        NES_TRACE2("Emitted windowIdentifier {}", windowIdentifier.toString());
    }
}

void NLJBuild::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("checkWindowsTriggerProxy", checkWindowsTriggerProxy,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWorkerContext(),
                           Value<Boolean>(isLeftSide));

}

void NLJBuild::execute(ExecutionContext& ctx, Record& record) const {
    // Get the global state
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // Get the memRef to the new entry
    auto entryMemRef = Nautilus::FunctionCall("insertEntryMemRefProxy", insertEntryMemRefProxy,
                                              operatorHandlerMemRef,
                                              Value<Boolean>(isLeftSide),
                                              record.read(timeStampField).as<UInt64>());

    // Write Record at entryMemRef
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto& field : schema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

NLJBuild::NLJBuild(uint64_t operatorHandlerIndex, const SchemaPtr &schema, const std::string &joinFieldName,
                   const std::string &timeStampField, bool isLeftSide) : operatorHandlerIndex(operatorHandlerIndex),
                                                                         schema(schema),
                                                                         joinFieldName(joinFieldName),
                                                                         timeStampField(timeStampField),
                                                                         isLeftSide(isLeftSide) {}

} // namespace NES::Runtime::Execution::Operators