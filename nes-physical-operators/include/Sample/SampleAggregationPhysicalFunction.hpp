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

#include <cstddef>
#include <Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

struct SampleAggregationState
{
};

class SampleAggregationPhysicalFunction
{
public:
    virtual ~SampleAggregationPhysicalFunction() = default;

    virtual void
    lift(nautilus::val<SampleAggregationState*> state, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record) const
        = 0;

    virtual void combine(
        nautilus::val<SampleAggregationState*> state1,
        nautilus::val<SampleAggregationState*> state2,
        PipelineMemoryProvider& pipelineMemoryProvider) const
        = 0;

    [[nodiscard]] virtual Record lower(nautilus::val<SampleAggregationState*> state, PipelineMemoryProvider& pipelineMemoryProvider) const
        = 0;

    [[nodiscard]] virtual nautilus::val<bool> isEmpty(nautilus::val<SampleAggregationState*> state) const = 0;

    /// Resets the state to its initial (empty) value. Called once, when a hashmap entry is first created.
    virtual void reset(nautilus::val<SampleAggregationState*> state, PipelineMemoryProvider& pipelineMemoryProvider) const = 0;

    /// Destroys the state. Used to free up memory when the state is no longer needed (e.g. a strategy that owns an external buffer).
    virtual void cleanup(nautilus::val<SampleAggregationState*> state) const = 0;

    [[nodiscard]] virtual size_t getSizeOfStateInBytes() const = 0;

    static void storeNull(const nautilus::val<SampleAggregationState*>& state, const nautilus::val<bool>& isNull);
    [[nodiscard]] static nautilus::val<bool> readNull(const nautilus::val<SampleAggregationState*>& state);
};

}
