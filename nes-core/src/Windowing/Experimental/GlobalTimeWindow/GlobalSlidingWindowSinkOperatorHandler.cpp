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
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Experimental/HashMap.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalSlice.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/WindowProcessingTasks.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
namespace NES::Windowing::Experimental {

GlobalSlidingWindowSinkOperatorHandler::GlobalSlidingWindowSinkOperatorHandler(
    const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
    std::shared_ptr<GlobalSliceStore<GlobalSlice>>& globalSliceStore)
    : globalSliceStore(globalSliceStore), windowDefinition(windowDefinition) {}

void GlobalSlidingWindowSinkOperatorHandler::setup(Runtime::Execution::PipelineExecutionContext&, uint64_t entrySize) {
    this->entrySize = entrySize;
}

void GlobalSlidingWindowSinkOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                                   Runtime::StateManagerPtr,
                                                   uint32_t) {
    NES_DEBUG("start GlobalSlidingWindowSinkOperatorHandler");
}

void GlobalSlidingWindowSinkOperatorHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                                  Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop GlobalSlidingWindowSinkOperatorHandler: " << queryTerminationType);
}

GlobalSlicePtr GlobalSlidingWindowSinkOperatorHandler::createGlobalSlice(WindowTriggerTask* windowTriggerTask) {
    return std::make_unique<GlobalSlice>(entrySize, windowTriggerTask->windowStart, windowTriggerTask->windowEnd);
}
std::vector<GlobalSliceSharedPtr>
GlobalSlidingWindowSinkOperatorHandler::getSlicesForWindow(WindowTriggerTask* windowTriggerTask) {
    NES_DEBUG("getSlicesForWindow " << windowTriggerTask->windowStart << " - " << windowTriggerTask->windowEnd);
    return globalSliceStore->getSlicesForWindow(windowTriggerTask->windowStart, windowTriggerTask->windowEnd);
}
Windowing::LogicalWindowDefinitionPtr GlobalSlidingWindowSinkOperatorHandler::getWindowDefinition() { return windowDefinition; }
GlobalSliceStore<GlobalSlice>& GlobalSlidingWindowSinkOperatorHandler::getGlobalSliceStore() { return *globalSliceStore; }
GlobalSlidingWindowSinkOperatorHandler::~GlobalSlidingWindowSinkOperatorHandler() {
    NES_DEBUG("Destruct GlobalSlidingWindowSinkOperatorHandler");
}

void GlobalSlidingWindowSinkOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {
    globalSliceStore.reset();
}

}// namespace NES::Windowing::Experimental