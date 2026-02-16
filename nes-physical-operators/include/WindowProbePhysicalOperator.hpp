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

#include <optional>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <CompilationContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// Is the general probe operator for window operators. It is responsible for garbage collecting slices and windows.
/// It is part of the second phase (probe) that processes the build up state of the first phase (build).
class WindowProbePhysicalOperator
{
public:
    explicit WindowProbePhysicalOperator(OperatorHandlerId operatorHandlerId, WindowMetaData windowMetaData);

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const;
    [[nodiscard]] WindowProbePhysicalOperator withChild(PhysicalOperator child) const;

    /// The setup method is called for each pipeline during the query initialization procedure. Meaning that if
    /// multiple pipelines with the same operator (e.g. JoinBuild) have access to the same operator handler, this will lead to race conditions.
    /// Therefore, any setup to the operator handler should ONLY happen in the WindowProbePhysicalOperator.
    void setup(ExecutionContext& ctx, CompilationContext& compCtx) const;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const;
    /// Checks the current watermark and then deletes all slices and windows that are not valid anymore
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const;
    void terminate(ExecutionContext& ctx) const;
    void execute(ExecutionContext& ctx, Record& record) const;

    [[nodiscard]] OperatorId getId() const;
    OperatorId id = INVALID_OPERATOR_ID;

protected:
    /// Helper classes to propagate to the child
    void setupChild(ExecutionContext& executionCtx, CompilationContext& compilationContext) const;
    void openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void executeChild(ExecutionContext& executionCtx, Record& record) const;
    void terminateChild(ExecutionContext& executionCtx) const;

protected:
    std::optional<PhysicalOperator> child;
    OperatorHandlerId operatorHandlerId;
    WindowMetaData windowMetaData;
};

static_assert(PhysicalOperatorConcept<WindowProbePhysicalOperator>);

}
