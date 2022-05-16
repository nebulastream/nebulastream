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

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateVariable.hpp>
#include <Util/Experimental/HashMap.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalSlice.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalWindowGlobalSliceStoreAppendOperatorHandler.hpp>
#include <Windowing/Experimental/LockFreeMultiOriginWatermarkProcessor.hpp>
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
#include <Windowing/Experimental/WindowProcessingTasks.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
namespace NES::Windowing::Experimental {

GlobalWindowGlobalSliceStoreAppendOperatorHandler::GlobalWindowGlobalSliceStoreAppendOperatorHandler(
    const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
    std::weak_ptr<GlobalSliceStore<GlobalSlice>> globalSliceStore)
    : globalSliceStore(globalSliceStore), windowDefinition(windowDefinition) {
    windowSize = windowDefinition->getWindowType()->getSize().getTime();
    windowSlide = windowDefinition->getWindowType()->getSlide().getTime();
}

void GlobalWindowGlobalSliceStoreAppendOperatorHandler::setup(Runtime::Execution::PipelineExecutionContext&,
                                                              NES::Experimental::HashMapFactoryPtr hashmapFactory) {
    this->factory = hashmapFactory;
}

void GlobalWindowGlobalSliceStoreAppendOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                                              Runtime::StateManagerPtr,
                                                              uint32_t) {
    NES_DEBUG("start GlobalWindowGlobalSliceStoreAppendOperatorHandler");
}

void GlobalWindowGlobalSliceStoreAppendOperatorHandler::triggerSliceMerging(Runtime::WorkerContext& wctx,
                                                                            Runtime::Execution::PipelineExecutionContext& ctx,
                                                                            uint64_t sequenceNumber,
                                                                            GlobalSlicePtr slice) {
    auto global = globalSliceStore.lock();
    NES_ASSERT(global, "globalSliceStore is not valid anymore");
    // add pre-aggregated slice to slice store
    auto windows = global->addSliceAndTriggerWindows(sequenceNumber, std::move(slice), windowSize, windowSlide);
    // check if we can trigger window computation
    for (auto& window : windows) {
        auto buffer = wctx.allocateTupleBuffer();
        auto task = buffer.getBuffer<WindowTriggerTask>();
        // we trigger the completion of all windows that end between startSlice and <= endSlice.
        NES_DEBUG("Deploy window trigger task for slice  ( " << window.startTs << "-" << window.endTs << ")");
        task->windowStart = window.startTs;
        task->windowEnd = window.endTs;
        task->sequenceNumber = window.sequenceNumber;
        buffer.setNumberOfTuples(1);
        ctx.dispatchBuffer(buffer);
    }
}

void GlobalWindowGlobalSliceStoreAppendOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr ctx) {
    NES_DEBUG("stop GlobalWindowGlobalSliceStoreAppendOperatorHandler");
    auto global = globalSliceStore.lock();
    if (!global) {
        return;
    }

    auto windows = global->triggerAllInflightWindows(windowSize, windowSlide);
    for (auto& window : windows) {
        auto buffer = ctx->getBufferManager()->getBufferBlocking();
        auto task = buffer.getBuffer<WindowTriggerTask>();
        // we trigger the completion of all windows that end between startSlice and <= endSlice.
        NES_DEBUG("Deploy window trigger task for slice  ( " << window.startTs << "-" << window.endTs << ")");
        task->windowStart = window.startTs;
        task->windowEnd = window.endTs;
        task->sequenceNumber = window.sequenceNumber;
        buffer.setNumberOfTuples(1);
        ctx->dispatchBuffer(buffer);
    }
}
GlobalWindowGlobalSliceStoreAppendOperatorHandler::~GlobalWindowGlobalSliceStoreAppendOperatorHandler() {
    NES_DEBUG("Destruct GlobalWindowGlobalSliceStoreAppendOperatorHandler");
}
Windowing::LogicalWindowDefinitionPtr GlobalWindowGlobalSliceStoreAppendOperatorHandler::getWindowDefinition() {
    return windowDefinition;
}

}// namespace NES::Windowing::Experimental