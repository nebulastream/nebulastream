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

#include "OperatorHandlerTracer.hpp"
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::Operators {
State::~State() = default;
NonKeyedSliceMergingHandler::NonKeyedSliceMergingHandler() {
    TRACE_OPERATOR_HANDLER("NES::Runtime::Execution::Operators::NonKeyedSliceMergingHandler",
                           "Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp");
}

void NonKeyedSliceMergingHandler::setup(PipelineExecutionContext&, uint64_t entrySize) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NonKeyedSliceMergingHandler::start(PipelineExecutionContextPtr, uint32_t) { NES_DEBUG("start NonKeyedSliceMergingHandler"); }

GlobalSlicePtr NonKeyedSliceMergingHandler::createGlobalSlice(SliceMergeTask<NonKeyedSlice>* sliceMergeTask) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

const State* NonKeyedSliceMergingHandler::getDefaultState() const { return defaultState.get(); }

NonKeyedSliceMergingHandler::~NonKeyedSliceMergingHandler() { NES_DEBUG("Destruct NonKeyedSliceMergingHandler"); }

void NonKeyedSliceMergingHandler::stop(Runtime::QueryTerminationType, PipelineExecutionContextPtr) {
    NES_DEBUG("stop NonKeyedSliceMergingHandler");
}

}// namespace NES::Runtime::Execution::Operators