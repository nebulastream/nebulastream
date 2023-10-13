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

#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSliceMergingOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSliceStaging.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Windowing/Experimental/WindowProcessingTasks.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
namespace NES::Windowing::Experimental {

KeyedSliceMergingOperatorHandler::KeyedSliceMergingOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition)
    : sliceStaging(std::make_shared<KeyedSliceStaging>()), windowDefinition(windowDefinition) {
    auto timeBasedWindowtype = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    windowSize = timeBasedWindowtype->getSize().getTime();
    windowSlide = timeBasedWindowtype->getSlide().getTime();
}

void KeyedSliceMergingOperatorHandler::setup(Runtime::Execution::PipelineExecutionContext&,
                                             NES::Experimental::HashMapFactoryPtr hashmapFactory) {
    this->factory = hashmapFactory;
}

void KeyedSliceMergingOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                             Runtime::StateManagerPtr,
                                             uint32_t) {
    NES_DEBUG("start KeyedSliceMergingOperatorHandler");
    activeCounter++;
}

void KeyedSliceMergingOperatorHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                            Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop KeyedSliceMergingOperatorHandler: {}", queryTerminationType);
}

KeyedSlicePtr KeyedSliceMergingOperatorHandler::createKeyedSlice(SliceMergeTask* sliceMergeTask) {
    return std::make_unique<KeyedSlice>(factory, sliceMergeTask->startSlice, sliceMergeTask->endSlice);
}
KeyedSliceMergingOperatorHandler::~KeyedSliceMergingOperatorHandler() { NES_DEBUG("Destruct SliceStagingWindowHandler"); }
Windowing::LogicalWindowDefinitionPtr KeyedSliceMergingOperatorHandler::getWindowDefinition() { return windowDefinition; }
std::weak_ptr<KeyedSliceStaging> KeyedSliceMergingOperatorHandler::getSliceStagingPtr() { return sliceStaging; }

void KeyedSliceMergingOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {
    this->sliceStaging->clear();
    this->sliceStaging.reset();
}

}// namespace NES::Windowing::Experimental