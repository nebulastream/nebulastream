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
#include <Windowing/Experimental/GlobalTimeBasedWindow/GlobalSlice.hpp>
#include <Windowing/Experimental/GlobalTimeBasedWindow/GlobalSliceMergingOperatorHandler.hpp>
#include <Windowing/Experimental/GlobalTimeBasedWindow/GlobalThreadLocalSliceStore.hpp>
#include <Windowing/Experimental/GlobalTimeBasedWindow/GlobalSliceStaging.hpp>
#include <Windowing/Experimental/WindowProcessingTasks.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
namespace NES::Windowing::Experimental {

GlobalSliceMergingOperatorHandler::GlobalSliceMergingOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition)
    : sliceStaging(std::make_shared<SliceStaging>()), windowDefinition(windowDefinition) {
    windowSize = windowDefinition->getWindowType()->getSize().getTime();
    windowSlide = windowDefinition->getWindowType()->getSlide().getTime();
}

void GlobalSliceMergingOperatorHandler::setup(Runtime::Execution::PipelineExecutionContext&,
                                             NES::Experimental::HashMapFactoryPtr hashmapFactory) {
    this->factory = hashmapFactory;
}

void GlobalSliceMergingOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                             Runtime::StateManagerPtr,
                                             uint32_t) {
    NES_DEBUG("start GlobalSliceMergingOperatorHandler");
    activeCounter++;
}

void GlobalSliceMergingOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop GlobalSliceMergingOperatorHandler");
    activeCounter--;
    if (activeCounter == 0) {
        NES_DEBUG("shutdown KeyedEventTimeWindowHandler");
        this->sliceStaging->clear();
        this->sliceStaging.reset();
    }
}

KeyedSlicePtr GlobalSliceMergingOperatorHandler::createKeyedSlice(SliceMergeTask* sliceMergeTask) {
    return std::make_unique<KeyedSlice>(factory, sliceMergeTask->startSlice, sliceMergeTask->endSlice);
}
GlobalSliceMergingOperatorHandler::~GlobalSliceMergingOperatorHandler() { NES_DEBUG("Destruct SliceStagingWindowHandler"); }
Windowing::LogicalWindowDefinitionPtr GlobalSliceMergingOperatorHandler::getWindowDefinition() { return windowDefinition; }
std::weak_ptr<SliceStaging> GlobalSliceMergingOperatorHandler::getSliceStagingPtr() { return sliceStaging; }

}// namespace NES::Windowing::Experimental