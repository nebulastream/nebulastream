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
#include <Join/IntervalJoin/IntervalJoinBuildPhysicalOperator.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Anchor-side (left input) build operator: registers/notifies/terminates against the handler's
/// anchor store. execute() is inherited from IntervalJoinBuildPhysicalOperator.
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

}
