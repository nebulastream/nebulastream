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

#pragma once


#include <memory>
#include <vector>
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Configurations/Worker/SliceCacheConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceCache/SliceCache.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <HashMapOptions.hpp>

namespace NES
{
class AggregationBuildCachePhysicalOperator;
int8_t* createAndAddAggregationSliceToCache(
    SliceCacheEntry* sliceCacheEntry,
    const AggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const AggregationBuildPhysicalOperator* buildOperator);
class AggregationBuildCachePhysicalOperator final : public AggregationBuildPhysicalOperator
{
public:
    AggregationBuildCachePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        std::unique_ptr<TimeFunction> timeFunction,
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
        HashMapOptions hashMapOptions,
        NES::Configurations::SliceCacheOptions sliceCacheOptions);
    ~AggregationBuildCachePhysicalOperator() override = default;
    void setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

private:
    /// This might not be the best place to store it, but it is an easy way to use them in this PoC branch
    NES::Configurations::SliceCacheOptions sliceCacheOptions;
};

}
