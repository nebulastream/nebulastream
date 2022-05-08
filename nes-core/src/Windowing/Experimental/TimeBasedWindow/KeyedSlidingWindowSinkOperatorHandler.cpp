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

#include <Runtime/WorkerContext.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/TimeBasedWindow/SliceStaging.hpp>
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

NES::Experimental::Hashmap KeyedSlidingWindowSinkOperatorHandler::getHashMap() { return factory->create(); }

void KeyedSlidingWindowSinkOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                                  Runtime::StateManagerPtr,
                                                  uint32_t) {
    NES_DEBUG("start KeyedSlidingWindowSinkOperatorHandler");
}

void KeyedSlidingWindowSinkOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop KeyedSlidingWindowSinkOperatorHandler");
    globalSliceStore->clear();
    globalSliceStore.reset();
}

KeyedSlicePtr KeyedSlidingWindowSinkOperatorHandler::createKeyedSlice(WindowTriggerTask* sliceMergeTask) {
    return std::make_unique<KeyedSlice>(factory, sliceMergeTask->startSlice, sliceMergeTask->endSlice);
}
std::vector<KeyedSliceSharedPtr> KeyedSlidingWindowSinkOperatorHandler::getSlicesForWindow(uint64_t startTs) {
    return globalSliceStore->getSlicesForWindow(startTs, startTs + windowSize);
};

}// namespace NES::Windowing::Experimental