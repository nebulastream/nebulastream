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
#include <OperatorHandlerTracer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
namespace NES::Runtime::Execution::Operators {

KeyedSliceMergingHandler::KeyedSliceMergingHandler() {
    TRACE_OPERATOR_HANDLER("NES::Runtime::Execution::Operators::KeyedSliceMergingHandler",
                           "handler/KeyedSliceMergingHandler.hpp");
}

void KeyedSliceMergingHandler::setup(Runtime::Execution::PipelineExecutionContext&, uint64_t keySize, uint64_t valueSize) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void KeyedSliceMergingHandler::start(Runtime::Execution::PipelineExecutionContextPtr, uint32_t) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void KeyedSliceMergingHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                    Runtime::Execution::PipelineExecutionContextPtr) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

KeyedSlicePtr KeyedSliceMergingHandler::createGlobalSlice(SliceMergeTask<KeyedSlice>* sliceMergeTask, uint64_t numberOfKeys) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
KeyedSliceMergingHandler::~KeyedSliceMergingHandler() { NES_DEBUG("Destruct SliceStagingWindowHandler"); }

}// namespace NES::Runtime::Execution::Operators