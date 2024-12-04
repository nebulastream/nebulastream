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
#include <Execution/Operators/Operator.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <val_ptr.hpp>

namespace NES::Runtime::Execution::Operators
{

/// This class is the first phase of the join. For both streams (left and right), the tuples are stored in the
/// corresponding slice one after the other. Afterward, the second phase (NLJProbe) will start joining the tuples
/// via two nested loops.
class NLJBuild final : public StreamJoinBuild
{
public:
    /// Local state stores all the necessary variables for the current slice,
    /// as we expect that more tuples will be inserted into this slice
    class LocalNestedLoopJoinState final : public OperatorState
    {
    public:
        LocalNestedLoopJoinState(
            const nautilus::val<OperatorHandler*>& operatorHandler,
            const nautilus::val<NLJSlice*>& sliceReference,
            const nautilus::val<SliceStart>& sliceStart,
            const nautilus::val<SliceEnd>& sliceEnd,
            const nautilus::val<Interface::PagedVector*>& nljPagedVectorMemRef)
            : joinOperatorHandler(operatorHandler)
            , sliceReference(sliceReference)
            , sliceStart(sliceStart)
            , sliceEnd(sliceEnd)
            , nljPagedVectorMemRef(nljPagedVectorMemRef) {};

        nautilus::val<NLJOperatorHandler*> joinOperatorHandler;
        nautilus::val<NLJSlice*> sliceReference;
        nautilus::val<SliceStart> sliceStart;
        nautilus::val<SliceEnd> sliceEnd;
        nautilus::val<Interface::PagedVector*> nljPagedVectorMemRef;
    };

    NLJBuild(
        uint64_t operatorHandlerIndex,
        QueryCompilation::JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;


private:
    /// Returns the populated local join state for the current timestamp
    LocalNestedLoopJoinState* getLocalJoinState(ExecutionContext& executionCtx, const nautilus::val<Timestamp>& timestamp) const;

    /// Updates the localJoinState by getting the values via nautilus::invoke()
    void updateLocalJoinState(
        LocalNestedLoopJoinState* localJoinState,
        const nautilus::val<NLJOperatorHandler*>& operatorHandlerRef,
        const ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& timestamp) const;
};
}
