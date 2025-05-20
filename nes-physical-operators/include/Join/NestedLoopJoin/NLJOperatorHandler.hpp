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
#include <functional>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <folly/Synchronized.h>
#include <PipelineExecutionContext.hpp>

namespace NES
{
/// This task models the information for a join window trigger
struct EmittedNLJWindowTrigger
{
    SliceEnd leftSliceEnd;
    SliceEnd rightSliceEnd;
    WindowInfo windowInfo;
};

class NLJOperatorHandler final : public StreamJoinOperatorHandler
{
public:
    static constexpr int64_t windowSizeRollingAverage = 10;
    NLJOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> leftMemoryProvider,
        std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> rightMemoryProvider);

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)> getCreateNewSlicesFunction() const override;

private:
    void emitSliceIdsToProbe(
        Slice& sliceLeft,
        Slice& sliceRight,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        PipelineExecutionContext* pipelineCtx) override;
};

/// Proxy function that returns the pointer to the correct PagedVector
Nautilus::Interface::PagedVector*
getNLJPagedVectorProxy(const NLJSlice* nljSlice, WorkerThreadId workerThreadId, JoinBuildSideType joinBuildSide);
}
