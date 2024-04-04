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
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <tuple>

namespace NES::Runtime::Execution::Operators {

KeyedSlicePreAggregationHandler::KeyedSlicePreAggregationHandler(uint64_t windowSize,
                                                                 uint64_t windowSlide,
                                                                 const std::vector<OriginId>& origins)
    : AbstractSlicePreAggregationHandler<KeyedSlice, KeyedThreadLocalSliceStore>(windowSize, windowSlide, origins) {}

void KeyedSlicePreAggregationHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx,
                                            uint64_t keySize,
                                            uint64_t valueSize) {
    NES_ASSERT(threadLocalSliceStores.empty(), "The thread local slice store must be empty");
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto threadLocal = std::make_unique<KeyedThreadLocalSliceStore>(keySize, valueSize, windowSize, windowSlide);
        threadLocalSliceStores.emplace_back(std::move(threadLocal));
    }
}

KeyedSlicePreAggregationHandler::~KeyedSlicePreAggregationHandler() { NES_DEBUG("~GlobalSlicePreAggregationHandler"); }

}// namespace NES::Runtime::Execution::Operators