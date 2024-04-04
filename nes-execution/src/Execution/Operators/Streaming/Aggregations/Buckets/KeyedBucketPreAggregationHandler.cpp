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

#include <Execution/Operators/Streaming/Aggregations/Buckets/KeyedBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <memory>
#include <tuple>

namespace NES::Runtime::Execution::Operators {

KeyedBucketPreAggregationHandler::KeyedBucketPreAggregationHandler(uint64_t windowSize,
                                                                   uint64_t windowSlide,
                                                                   const std::vector<OriginId>& origins)
    : AbstractBucketPreAggregationHandler<KeyedSlice, KeyedBucketStore>(windowSize, windowSlide, origins) {}

void KeyedBucketPreAggregationHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx,
                                             uint64_t keySize,
                                             uint64_t valueSize) {
    NES_ASSERT(threadLocalBuckets.empty(), "The thread local slice store must be empty");
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto threadLocal = std::make_unique<KeyedBucketStore>(keySize, valueSize, windowSize, windowSlide);
        threadLocalBuckets.emplace_back(std::move(threadLocal));
    }
}

KeyedBucketPreAggregationHandler::~KeyedBucketPreAggregationHandler() { NES_DEBUG("~KeyedBucketPreAggregationHandler"); }

}// namespace NES::Runtime::Execution::Operators