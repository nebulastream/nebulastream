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
#include <Windowing/Experimental/LockFreeMultiOriginWatermarkProcessor.hpp>
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedGlobalSliceStore.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedThreadLocalSliceStore.hpp>
#include <Windowing/Experimental/TimeBasedWindow/SliceStaging.hpp>
#include <Windowing/Experimental/TimeBasedWindow/WindowProcessingTasks.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
namespace NES::Windowing::Experimental {

KeyedSlidingWindowSinkOperatorHandler::KeyedSlidingWindowSinkOperatorHandler(
    const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
    std::shared_ptr<KeyedGlobalSliceStore>& globalSliceStore)
    : globalSliceStore(globalSliceStore), windowDefinition(windowDefinition) {
    windowSize = windowDefinition->getWindowType()->getSize().getTime();
    windowSlide = windowDefinition->getWindowType()->getSlide().getTime();
}

void KeyedSlidingWindowSinkOperatorHandler::setup(Runtime::Execution::PipelineExecutionContext&,
                                                  NES::Experimental::HashMapFactoryPtr hashmapFactory) {
    this->factory = hashmapFactory;
}

void KeyedSlidingWindowSinkOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                                  Runtime::StateManagerPtr,
                                                  uint32_t) {
    NES_DEBUG("start KeyedSlidingWindowSinkOperatorHandler");
}

void KeyedSlidingWindowSinkOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop KeyedSlidingWindowSinkOperatorHandler");
    globalSliceStore.reset();
}

KeyedSlicePtr KeyedSlidingWindowSinkOperatorHandler::createKeyedSlice(WindowTriggerTask* windowTriggerTask) {
    return std::make_unique<KeyedSlice>(factory, windowTriggerTask->windowStart, windowTriggerTask->windowEnd);
}
std::vector<KeyedSliceSharedPtr> KeyedSlidingWindowSinkOperatorHandler::getSlicesForWindow(WindowTriggerTask* windowTriggerTask) {
    NES_DEBUG("getSlicesForWindow " << windowTriggerTask->windowStart << " - " << windowTriggerTask->windowEnd);
    return globalSliceStore->getSlicesForWindow(windowTriggerTask->windowStart, windowTriggerTask->windowEnd);
}
Windowing::LogicalWindowDefinitionPtr KeyedSlidingWindowSinkOperatorHandler::getWindowDefinition() { return windowDefinition; }

}// namespace NES::Windowing::Experimental