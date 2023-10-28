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

#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Windowing/Experimental/GlobalSliceStore.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Windowing/Experimental/WindowProcessingTasks.hpp>
namespace NES::Windowing::Experimental {

KeyedSlidingWindowSinkOperatorHandler::KeyedSlidingWindowSinkOperatorHandler(
    const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
    std::shared_ptr<GlobalSliceStore<KeyedSlice>>& globalSliceStore)
    : globalSliceStore(globalSliceStore), windowDefinition(windowDefinition) {
    auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    windowSize = timeBasedWindowType->getSize().getTime();
    windowSlide = timeBasedWindowType->getSlide().getTime();
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

void KeyedSlidingWindowSinkOperatorHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                                 Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop KeyedSlidingWindowSinkOperatorHandler: {}", queryTerminationType);
}

KeyedSlicePtr KeyedSlidingWindowSinkOperatorHandler::createKeyedSlice(WindowTriggerTask* windowTriggerTask) {
    return std::make_unique<KeyedSlice>(factory, windowTriggerTask->windowStart, windowTriggerTask->windowEnd);
}
std::vector<KeyedSliceSharedPtr> KeyedSlidingWindowSinkOperatorHandler::getSlicesForWindow(WindowTriggerTask* windowTriggerTask) {
    NES_DEBUG("getSlicesForWindow {} - {}", windowTriggerTask->windowStart, windowTriggerTask->windowEnd);
    return globalSliceStore->getSlicesForWindow(windowTriggerTask->windowStart, windowTriggerTask->windowEnd);
}
Windowing::LogicalWindowDefinitionPtr KeyedSlidingWindowSinkOperatorHandler::getWindowDefinition() { return windowDefinition; }

GlobalSliceStore<KeyedSlice>& KeyedSlidingWindowSinkOperatorHandler::getGlobalSliceStore() { return *globalSliceStore; }
KeyedSlidingWindowSinkOperatorHandler::~KeyedSlidingWindowSinkOperatorHandler() {
    NES_DEBUG("Destruct KeyedEventTimeWindowHandler");
}

void KeyedSlidingWindowSinkOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {
    globalSliceStore.reset();
}

std::string KeyedSlidingWindowSinkOperatorHandler::toString() const {
    std::stringstream out;
    out << windowDefinition->toString();
    out << "windowSize: " << windowSize << "\n";
    out << "windowSlide: " << windowSlide << "\n";
    out << std::endl;
    return out.str();
}

}// namespace NES::Windowing::Experimental