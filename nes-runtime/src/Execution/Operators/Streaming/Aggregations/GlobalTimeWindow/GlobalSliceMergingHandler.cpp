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

#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
namespace NES::Runtime::Execution::Operators {

GlobalSliceMergingHandler::GlobalSliceMergingHandler(std::shared_ptr<GlobalSliceStaging> globalSliceStaging)
    : sliceStaging(globalSliceStaging) {}

void GlobalSliceMergingHandler::setup(Runtime::Execution::PipelineExecutionContext&, uint64_t entrySize) {
    this->entrySize = entrySize;
    defaultState = std::make_unique<State>(entrySize);
    defaultState = std::make_unique<State>(entrySize);
}

void GlobalSliceMergingHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG("start GlobalSliceMergingHandler");
}

void GlobalSliceMergingHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                     Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop GlobalSliceMergingHandler: " << queryTerminationType);
}

GlobalSlicePtr GlobalSliceMergingHandler::createGlobalSlice(SliceMergeTask* sliceMergeTask) {
    return std::make_unique<GlobalSlice>(entrySize, sliceMergeTask->startSlice, sliceMergeTask->endSlice, defaultState);
}
const State* GlobalSliceMergingHandler::getDefaultState() const { return defaultState.get(); }

GlobalSliceMergingHandler::~GlobalSliceMergingHandler() { NES_DEBUG("Destruct SliceStagingWindowHandler"); }
std::weak_ptr<GlobalSliceStaging> GlobalSliceMergingHandler::getSliceStagingPtr() { return sliceStaging; }

}// namespace NES::Runtime::Execution::Operators