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
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Build-side operator for the streaming interval join. One class serves both inputs, parameterized on
/// buildSide. Each tuple is appended to the slice covering its timestamp. Setup, per-buffer close, and
/// terminate are overridden because the base paths dynamic_cast the handler to WindowBasedOperatorHandler,
/// which the interval-join handler does not extend.
class IntervalJoinBuildPhysicalOperator final : public StreamJoinBuildPhysicalOperator
{
public:
    IntervalJoinBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        IntervalJoinBuildSide buildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
        std::unique_ptr<SliceStoreRef> sliceStoreRef);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void terminate(ExecutionContext& executionCtx) const override;

private:
    IntervalJoinBuildSide buildSide;
};

}
