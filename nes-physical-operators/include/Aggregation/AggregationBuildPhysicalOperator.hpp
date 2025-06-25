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
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <Engine.hpp>
#include <HashMapOptions.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{
class AggregationBuildPhysicalOperator;
Interface::HashMap* getAggHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    Timestamp timestamp,
    WorkerThreadId workerThreadId,
    const AggregationBuildPhysicalOperator* buildOperator);
class AggregationBuildPhysicalOperator final : public WindowBuildPhysicalOperator
{
public:
    friend Interface::HashMap* getAggHashMapProxy(
        const AggregationOperatorHandler* operatorHandler,
        Timestamp timestamp,
        WorkerThreadId workerThreadId,
        const AggregationBuildPhysicalOperator* buildOperator);

    AggregationBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        std::unique_ptr<TimeFunction> timeFunction,
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
        HashMapOptions hashMapOptions);
    void setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

private:
    /// The aggregation function is a shared_ptr, because it is used in the aggregation build and in the getSliceCleanupFunction()
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    HashMapOptions hashMapOptions;
};

}
