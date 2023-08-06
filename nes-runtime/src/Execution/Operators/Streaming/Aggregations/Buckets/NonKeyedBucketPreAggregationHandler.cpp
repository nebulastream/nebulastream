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

#include <Execution/Operators/Streaming/Aggregations/Buckets/NonKeyedBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/AbstractBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators {

NonKeyedBucketPreAggregationHandler::NonKeyedBucketPreAggregationHandler(uint64_t windowSize,
                                                                         uint64_t windowSlide,
                                                                         const std::vector<OriginId>& origins)
    : AbstractBucketPreAggregationHandler<NonKeyedSlice, NonKeyedBucketStore>(windowSize, windowSlide, origins) {}

void NonKeyedBucketPreAggregationHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize) {
    defaultState = std::make_unique<State>(entrySize);
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto threadLocal = std::make_unique<NonKeyedBucketStore>(entrySize, windowSize, windowSlide, defaultState);
        threadLocalBuckets.emplace_back(std::move(threadLocal));
    }
}

const State* NonKeyedBucketPreAggregationHandler::getDefaultState() const { return defaultState.get(); }

}// namespace NES::Runtime::Execution::Operators