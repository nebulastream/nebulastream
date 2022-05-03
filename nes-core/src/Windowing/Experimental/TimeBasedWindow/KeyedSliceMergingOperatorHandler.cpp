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
#include <Windowing/Experimental/TimeBasedWindow/SliceStaging.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSliceMergingOperatorHandler.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
namespace NES::Windowing::Experimental {

KeyedSliceMergingOperatorHandler::KeyedSliceMergingOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition)
    : windowDefinition(windowDefinition) {
    windowSize = windowDefinition->getWindowType()->getSize().getTime();
    windowSlide = windowDefinition->getWindowType()->getSlide().getTime();
}

void KeyedSliceMergingOperatorHandler::setup(Runtime::Execution::PipelineExecutionContext&,
                                      NES::Experimental::HashMapFactoryPtr hashmapFactory) {
    this->factory = hashmapFactory;
}

NES::Experimental::Hashmap KeyedSliceMergingOperatorHandler::getHashMap() { return factory->create(); }

void KeyedSliceMergingOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG("start KeyedSliceMergingOperatorHandler");
    activeCounter++;
}

void KeyedSliceMergingOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop KeyedSliceMergingOperatorHandler");
    activeCounter--;
    if (activeCounter == 0) {
        NES_DEBUG("shutdown KeyedEventTimeWindowHandler");
        this->sliceStaging->clear();
        this->sliceStaging.reset();
    }
}

KeyedSlicePtr KeyedSliceMergingOperatorHandler::createKeyedSlice(uint64_t sliceIndex) {
    auto startTs = sliceIndex;
    auto endTs = (sliceIndex + 1);
    return std::make_unique<KeyedSlice>(factory, startTs, endTs);
};

}// namespace NES::Windowing::Experimental