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

#include <Execution/Operators/Streaming/Aggregations/GlobalSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/WindowProcessingTasks.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
namespace NES::Runtime::Execution::Operators {

GlobalSliceMergingHandler::GlobalSliceMergingHandler() : sliceStaging(std::make_shared<GlobalSliceStaging>()) {}

void GlobalSliceMergingHandler::setup(Runtime::Execution::PipelineExecutionContext&, uint64_t entrySize) {
    this->entrySize = entrySize;
}

void GlobalSliceMergingHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                              Runtime::StateManagerPtr,
                                              uint32_t) {
    NES_DEBUG("start GlobalSliceMergingHandler");
}

void GlobalSliceMergingHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                             Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop GlobalSliceMergingHandler: " << queryTerminationType);
}

GlobalSlicePtr GlobalSliceMergingHandler::createGlobalSlice(SliceMergeTask* sliceMergeTask) {
    return std::make_unique<GlobalSlice>(entrySize, sliceMergeTask->startSlice, sliceMergeTask->endSlice);
}
GlobalSliceMergingHandler::~GlobalSliceMergingHandler() { NES_DEBUG("Destruct SliceStagingWindowHandler"); }
std::weak_ptr<GlobalSliceStaging> GlobalSliceMergingHandler::getSliceStagingPtr() { return sliceStaging; }

}// namespace NES::Runtime::Execution::Operators