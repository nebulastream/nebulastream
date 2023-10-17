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
#include <Runtime/WorkerContext.hpp>
#include <Windowing/Experimental/GlobalSliceStore.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedGlobalSliceStoreAppendOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Windowing/Experimental/WindowProcessingTasks.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
namespace NES::Windowing::Experimental {

KeyedGlobalSliceStoreAppendOperatorHandler::KeyedGlobalSliceStoreAppendOperatorHandler(
    const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
    std::weak_ptr<GlobalSliceStore<KeyedSlice>> globalSliceStore)
    : globalSliceStore(globalSliceStore), windowDefinition(windowDefinition) {
    auto windowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    windowSize = windowType->getSize().getTime();
    windowSlide = windowType->getSlide().getTime();
}

void KeyedGlobalSliceStoreAppendOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                                       Runtime::StateManagerPtr,
                                                       uint32_t) {
    NES_DEBUG("start KeyedGlobalSliceStoreAppendOperatorHandler");
}

void KeyedGlobalSliceStoreAppendOperatorHandler::triggerSliceMerging(Runtime::WorkerContext& wctx,
                                                                     Runtime::Execution::PipelineExecutionContext& ctx,
                                                                     uint64_t sequenceNumber,
                                                                     KeyedSlicePtr slice) {
    auto global = globalSliceStore.lock();
    if (!global) {
        NES_FATAL_ERROR("GlobalSliceStore is invalid, this should only happen after a hard stop. Drop all in flight data.");
        return;
    }
    // add pre-aggregated slice to slice store
    auto windows = global->addSliceAndTriggerWindows(sequenceNumber, std::move(slice), windowSize, windowSlide);
    // check if we can trigger window computation
    for (auto& window : windows) {
        auto buffer = wctx.allocateTupleBuffer();
        auto task = buffer.getBuffer<WindowTriggerTask>();
        // we trigger the completion of all windows that end between startSlice and <= endSlice.
        NES_DEBUG("Deploy window trigger task for slice  ( {}-{})", window.startTs, window.endTs);
        task->windowStart = window.startTs;
        task->windowEnd = window.endTs;
        task->sequenceNumber = window.sequenceNumber;
        buffer.setNumberOfTuples(1);
        ctx.dispatchBuffer(buffer);
    }
}

void KeyedGlobalSliceStoreAppendOperatorHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                                      Runtime::Execution::PipelineExecutionContextPtr ctx) {
    NES_DEBUG("stop KeyedGlobalSliceStoreAppendOperatorHandler : {}", queryTerminationType);
    if (queryTerminationType == Runtime::QueryTerminationType::Graceful) {
        auto global = globalSliceStore.lock();
        NES_ASSERT(global, "Global slice store is invalid. This should not happen in a graceful stop.");
        auto windows = global->triggerAllInflightWindows(windowSize, windowSlide);
        for (auto& window : windows) {
            auto buffer = ctx->getBufferManager()->getBufferBlocking();
            auto task = buffer.getBuffer<WindowTriggerTask>();
            // we trigger the completion of all windows that end between startSlice and <= endSlice.
            NES_DEBUG("Deploy window trigger task for slice  ( {}-{})", window.startTs, window.endTs);
            task->windowStart = window.startTs;
            task->windowEnd = window.endTs;
            task->sequenceNumber = window.sequenceNumber;
            buffer.setNumberOfTuples(1);
            ctx->dispatchBuffer(buffer);
        }
    }
}
KeyedGlobalSliceStoreAppendOperatorHandler::~KeyedGlobalSliceStoreAppendOperatorHandler() {
    NES_DEBUG("Destruct KeyedEventTimeWindowHandler");
}

Windowing::LogicalWindowDefinitionPtr KeyedGlobalSliceStoreAppendOperatorHandler::getWindowDefinition() {
    return windowDefinition;
}

}// namespace NES::Windowing::Experimental