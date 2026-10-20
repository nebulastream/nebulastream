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
#include <SliceStore/SliceStoreRef.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>
#include <val.hpp>

namespace NES
{

/// Stores all necessary intermediates for window operator build to reduce calling proxy functions for each tuple.
class WindowOperatorBuildLocalState : public OperatorState
{
public:
    WindowOperatorBuildLocalState(const nautilus::val<OperatorHandler*>& operatorHandler, const nautilus::val<Timestamp>& lowerBound)
        : operatorHandler(operatorHandler), lowerBound(lowerBound)
    {
    }

    nautilus::val<OperatorHandler*> getOperatorHandler() { return operatorHandler; }

    /// Timestamps of an in-order buffer are never below the watermark its origin had when the buffer was opened.
    nautilus::val<Timestamp> getLowerBound() { return lowerBound; }

private:
    nautilus::val<OperatorHandler*> operatorHandler;
    nautilus::val<Timestamp> lowerBound;
};

/// Is the general probe operator for window operators. It is responsible for emitting slices and windows to the second phase (probe).
/// It is part of the first phase (build) that builds up the state of the window operator.
class WindowBuildPhysicalOperator : public PhysicalOperatorConcept
{
public:
    explicit WindowBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId, std::unique_ptr<TimeFunction> timeFunction, std::unique_ptr<SliceStoreRef> sliceStoreRef);
    WindowBuildPhysicalOperator(const WindowBuildPhysicalOperator& other);

    /// This setup function can be called in a multithreaded environment. Meaning that if
    /// multiple pipelines with the same operator (e.g. JoinBuild) have access to the same operator handler, this will lead to race conditions.
    /// Therefore, any setup to the operator handler should happen in the WindowProbePhysicalOperator.
    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;

    /// Initializes the time function, e.g., method that extracts the timestamp from a record
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    /// Passes emits slices that are ready to the second phase (probe) for further processing
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    /// Windows are triggered by watermarks, including the infinite watermark at the end of a stream, so there is nothing left to flush.
    void terminate(ExecutionContext&) const override { }

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

protected:
    /// The slice cache and the garbage collection rely on in-order timestamps, thus a timestamp below the lower bound of the
    /// current buffer fails the query.
    void failIfOutOfOrder(ExecutionContext& executionCtx, const nautilus::val<Timestamp>& timestamp) const;

    std::optional<PhysicalOperator> child;
    const OperatorHandlerId operatorHandlerId;
    const std::unique_ptr<TimeFunction> timeFunction;
    std::unique_ptr<SliceStoreRef> sliceStoreRef;
};

}
