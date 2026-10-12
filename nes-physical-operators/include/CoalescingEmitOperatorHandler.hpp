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

#include <chrono>
#include <cstddef>
#include <utility>
#include <Interface/BufferRef/BufferMerge.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <EmitOperatorHandler.hpp>
#include <PipelineExecutionContext.hpp>
#include <RangeCoalescer.hpp>

namespace NES
{

/// Handler of a `CoalescingEmitPhysicalOperator`. Its stop emits all outputs the coalescer still holds.
class CoalescingEmitOperatorHandler final : public EmitOperatorHandler
{
public:
    CoalescingEmitOperatorHandler(BufferLayout layout, const std::chrono::microseconds maxDelay, const size_t maxHeldRuns)
        : coalescer(std::move(layout), maxDelay, maxHeldRuns)
    {
    }

    void stop(QueryTerminationType, PipelineExecutionContext& pipelineExecutionContext) override
    {
        coalescer.flushAll(pipelineExecutionContext);
    }

    RangeCoalescer coalescer;
};

}
