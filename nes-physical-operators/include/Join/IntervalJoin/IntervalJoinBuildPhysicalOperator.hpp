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
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Build-side physical operator for the streaming interval join.
///
/// One class parameterized by JoinBuildSideType — both sides do the same
/// thing (append to the slice covering the tuple's timestamp). No interval
/// predicate enforcement here; that happens at probe time.
///
/// We inherit the StreamJoinBuildPhysicalOperator skeleton but override
/// setup/close/terminate because the inherited paths dynamic_cast the
/// handler to WindowBasedOperatorHandler, which our handler does not
/// extend. open() and execute() are safe to inherit (open) or define
/// fresh (execute).
class IntervalJoinBuildPhysicalOperator final : public StreamJoinBuildPhysicalOperator
{
public:
    IntervalJoinBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<TupleBufferRef> bufferRef,
        std::unique_ptr<SliceStoreRef> sliceStoreRef);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;
};

}
