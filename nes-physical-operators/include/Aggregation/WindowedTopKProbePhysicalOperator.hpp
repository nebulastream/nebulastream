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
#include <Aggregation/Function/TopKAggregationPhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <WindowProbePhysicalOperator.hpp>

namespace NES
{

/// Probe of ORDER BY ... LIMIT k BY ... with a tumbling or sliding window. For every triggered window, it merges the per-thread top
/// rows of its slices per key and emits the first k rows of every key in their order, with the window's start and end. A window is
/// final when it is triggered, so the output is append-only.
class WindowedTopKProbePhysicalOperator final : public WindowProbePhysicalOperator
{
public:
    WindowedTopKProbePhysicalOperator(
        ChainedHashMapConfig hashMapConfig,
        std::shared_ptr<TopKAggregationPhysicalFunction> topK,
        OperatorHandlerId operatorHandlerId,
        WindowMetaData windowMetaData);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;

protected:
    void probeWindow(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    ChainedHashMapConfig hashMapConfig;
    std::shared_ptr<TopKAggregationPhysicalFunction> topK;
};

}
