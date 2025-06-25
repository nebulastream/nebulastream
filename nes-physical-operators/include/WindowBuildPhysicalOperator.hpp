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
#include <optional>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Watermark/TimeFunction.hpp>
#include <Engine.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>
#include <val.hpp>

namespace NES
{

/// Stores all necessary intermediates for window operator build to reduce calling proxy functions for each tuple.
class WindowOperatorBuildLocalState : public OperatorState
{
public:
    explicit WindowOperatorBuildLocalState(const nautilus::val<OperatorHandler*>& operatorHandler) : operatorHandler(operatorHandler) { }
    nautilus::val<OperatorHandler*> getOperatorHandler() { return operatorHandler; }

private:
    nautilus::val<OperatorHandler*> operatorHandler;
};

/// Is the general probe operator for window operators. It is responsible for emitting slices and windows to the second phase (probe).
/// It is part of the first phase (build) that builds up the state of the window operator.
class WindowBuildPhysicalOperator : public PhysicalOperatorConcept
{
public:
    explicit WindowBuildPhysicalOperator(OperatorHandlerId operatorHandlerId, std::unique_ptr<TimeFunction> timeFunction);
    WindowBuildPhysicalOperator(const WindowBuildPhysicalOperator& other);

    /// This setup function can be called in a multithreaded environment. Meaning that if
    /// multiple pipelines with the same operator (e.g. JoinBuild) have access to the same operator handler, this will lead to race conditions.
    /// Therefore, any setup to the operator handler should happen in the WindowProbePhysicalOperator.
    void setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const override;

    /// Initializes the time function, e.g., method that extracts the timestamp from a record
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    /// Passes emits slices that are ready to the second phase (probe) for further processing
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    /// Emits/Flushes all slices and windows, as the query will be terminated
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

protected:
    std::optional<PhysicalOperator> child;
    const OperatorHandlerId operatorHandlerId;
    const std::unique_ptr<TimeFunction> timeFunction;
};

}
