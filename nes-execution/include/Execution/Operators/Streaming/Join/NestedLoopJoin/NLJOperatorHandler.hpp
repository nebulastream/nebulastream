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

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators
{

/// This task models the information for a join window trigger
struct EmittedNLJWindowTriggerTask
{
    SliceEnd leftSliceEnd;
    SliceEnd rightSliceEnd;
    WindowInfo windowInfo;
};

class NLJOperatorHandler final : public StreamJoinOperatorHandler
{
public:
    NLJOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        const OriginId outputOriginId,
        const uint64_t windowSize,
        const uint64_t windowSlide,
        const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
        const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider);

private:
    std::shared_ptr<StreamJoinSlice> createNewSlice(SliceStart sliceStart, SliceEnd sliceEnd) override;

    void emitSliceIdsToProbe(
        StreamJoinSlice& sliceLeft,
        StreamJoinSlice& sliceRight,
        const WindowInfo& windowInfo,
        PipelineExecutionContext* pipelineCtx) override;
};

/// Proxy function that returns the pointer to the correct PagedVector
Nautilus::Interface::PagedVector* getNLJPagedVectorProxy(
    const NLJSlice* nljSlice, WorkerThreadId::Underlying workerThreadId, QueryCompilation::JoinBuildSideType joinBuildSide);

}
