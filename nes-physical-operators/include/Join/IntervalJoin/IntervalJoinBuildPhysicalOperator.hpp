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
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Build-side skeleton for the streaming interval join.
///
/// Both sides do the same thing in execute() (append the tuple to the slice covering its
/// timestamp); only the setup/close/terminate wiring differs because it targets a specific
/// per-side store and notification on the handler. That side-specific wiring lives in the
/// IntervalJoinBuildAnchorPhysicalOperator / IntervalJoinBuildPartnerPhysicalOperator
/// subclasses, so neither carries a runtime side flag.
///
/// We inherit the StreamJoinBuildPhysicalOperator skeleton but the subclasses override
/// setup/close/terminate because the inherited paths dynamic_cast the handler to
/// WindowBasedOperatorHandler, which the interval-join handler does not extend.
class IntervalJoinBuildPhysicalOperator : public StreamJoinBuildPhysicalOperator
{
public:
    IntervalJoinBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<TupleBufferRef> bufferRef,
        std::unique_ptr<SliceStoreRef> sliceStoreRef);

    void execute(ExecutionContext& executionCtx, Record& record) const override;
};

// todo have separate files (.hpp/.cpp) for IntervalJoinBuildAnchorPhysicalOperator and IntervalJoinBuildPartnerPhysicalOperator
/// Anchor-side (left input) build operator: registers/notifies/terminates against the
/// handler's anchor store.
class IntervalJoinBuildAnchorPhysicalOperator final : public IntervalJoinBuildPhysicalOperator
{
public:
    IntervalJoinBuildAnchorPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<TupleBufferRef> bufferRef,
        std::unique_ptr<SliceStoreRef> sliceStoreRef);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;
};

/// Partner-side (right input) build operator: registers/notifies/terminates against the
/// handler's partner store.
class IntervalJoinBuildPartnerPhysicalOperator final : public IntervalJoinBuildPhysicalOperator
{
public:
    IntervalJoinBuildPartnerPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<TupleBufferRef> bufferRef,
        std::unique_ptr<SliceStoreRef> sliceStoreRef);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;
};

}
