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
#include <cstdint>
#include <memory>
#include <ostream>
#include <Runtime/TupleBuffer.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// The ExecutablePipelineStage is the interface that describes a processing step within a stream processing query.
class ExecutablePipelineStage
{
public:
    virtual ~ExecutablePipelineStage() = default;
    /// Prepares the ExecutablePipelineStage for future execution.
    /// `start` may throw to indicate an error.
    virtual void start(PipelineExecutionContext& pipelineExecutionContext) = 0;

    /// Executes the ExecutablePipelineStage with a readonly input tuple buffer.
    /// `execute` should never be called on a pipeline that has not previously been `started`.
    virtual void execute(const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) = 0;

    /// Stops the ExecutablePipelineStage allowing it to flush left over state.
    /// `stop` should never be called on a pipeline that has not previously been `started`.
    /// `stop` is not guaranteed to be called, thus the destructor should take care of cleanup.
    /// `stop` may throw to indicate an error.
    virtual void stop(PipelineExecutionContext& pipelineExecutionContext) = 0;

    friend std::ostream& operator<<(std::ostream& os, const ExecutablePipelineStage& eps) { return eps.toString(os); }

protected:
    virtual std::ostream& toString(std::ostream& os) const = 0;
};

}
