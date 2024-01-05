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
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators {
static constexpr uint64_t STATE_ALIGNMENT = 8;

BatchAggregationHandler::BatchAggregationHandler() = default;

BatchAggregationHandler::State BatchAggregationHandler::getThreadLocalState(uint64_t workerId) {
    auto index = workerId % threadLocalStateStores.size();
    return threadLocalStateStores[index];
}

void BatchAggregationHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize) {
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto* ptr = (State) std::aligned_alloc(STATE_ALIGNMENT, entrySize);
        std::memset(ptr, 0, entrySize);
        threadLocalStateStores.emplace_back(ptr);
    }
}

void BatchAggregationHandler::start(Runtime::Execution::PipelineExecutionContextPtr, uint32_t) {
    NES_DEBUG("start BatchAggregationHandler");
}

void BatchAggregationHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                   Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("shutdown BatchAggregationHandler: {}", queryTerminationType);
}
BatchAggregationHandler::~BatchAggregationHandler() {
    NES_DEBUG("~BatchAggregationHandler");

    for (auto s : threadLocalStateStores) {
        free(s);
    }
}

void BatchAggregationHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {}

}// namespace NES::Runtime::Execution::Operators