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

#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
namespace NES::Runtime::Execution::Operators {

KeyedSliceMergingHandler::KeyedSliceMergingHandler() {}

void KeyedSliceMergingHandler::setup(Runtime::Execution::PipelineExecutionContext&, uint64_t keySize, uint64_t valueSize) {
    this->keySize = keySize;
    this->valueSize = valueSize;
}

void KeyedSliceMergingHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG("start GlobalSliceMergingHandler");
}

void KeyedSliceMergingHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                    Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop GlobalSliceMergingHandler: {}", queryTerminationType);
}

KeyedSlicePtr KeyedSliceMergingHandler::createGlobalSlice(SliceMergeTask<KeyedSlice>* sliceMergeTask, uint64_t numberOfKeys) {
    // allocate hash map
    auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
    auto hashMap = std::make_unique<Nautilus::Interface::ChainedHashMap>(keySize, valueSize, numberOfKeys, std::move(allocator));
    return std::make_unique<KeyedSlice>(std::move(hashMap), sliceMergeTask->startSlice, sliceMergeTask->endSlice);
}
KeyedSliceMergingHandler::~KeyedSliceMergingHandler() { NES_DEBUG("Destruct SliceStagingWindowHandler"); }


}// namespace NES::Runtime::Execution::Operators