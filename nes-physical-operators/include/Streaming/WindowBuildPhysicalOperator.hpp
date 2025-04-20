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
#include <Abstract/PhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>
#include <AbstractPhysicalOperator.hpp>

namespace NES
{

/// Is the general probe operator for window operators. It is responsible for emitting slices and windows to the second phase (probe).
/// It is part of the first phase (build) that builds up the state of the window operator.
class WindowBuildPhysicalOperator : public PhysicalOperatorConcept
{
public:
    explicit WindowBuildPhysicalOperator(uint64_t operatorHandlerIndex, std::shared_ptr<TimeFunction> timeFunction);

    /// This setup function can be called in a multithreaded environment. Meaning that if
    /// multiple pipelines with the same operator (e.g. JoinBuild) have access to the same operator handler, this will lead to race conditions.
    /// Therefore, any setup to the operator handler should happen in the WindowProbePhysicalOperator.
    void setup(ExecutionContext& executionCtx) const override { PhysicalOperatorConcept::setup(executionCtx); };

    /// Initializes the time function, e.g., method that extracts the timestamp from a record
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    /// Passes emits slices that are ready to the second phase (probe) for further processing
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    /// Emits/Flushes all slices and windows, as the query will be terminated
    void terminate(ExecutionContext& executionCtx) const override;
    void setChild(PhysicalOperator child) override { this->child = child; }
    void setChild(struct PhysicalOperator child) override { this->child = child; }

protected:
    std::optional<PhysicalOperator> child;
    const uint64_t operatorHandlerIndex;
    const std::shared_ptr<TimeFunction> timeFunction;
};

}
