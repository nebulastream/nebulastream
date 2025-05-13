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
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Execution/Operators/Streaming/HashMapSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Execution.hpp>
#include <Engine.hpp>


namespace NES::Runtime::Execution::Operators
{

/// This task models the information for a hash join based window trigger
struct EmittedHJWindowTrigger
{
    WindowInfo windowInfo;
    uint64_t leftNumberOfHashMaps;
    uint64_t rightNumberOfHashMaps;
    Interface::HashMap** leftHashMaps; /// Pointer to the stored pointers of all hash maps that the probe should iterate over
    Interface::HashMap** rightHashMaps; /// Pointer to the stored pointers of all hash maps that the probe should iterate over
};


class HJOperatorHandler final : public StreamJoinOperatorHandler
{
public:
HJOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
        const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider)
        : StreamJoinOperatorHandler(
              inputOrigins, std::move(outputOriginId), std::move(sliceAndWindowStore), leftMemoryProvider, rightMemoryProvider)
    {
    }
    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction() const override;

    using NautilusCleanupExec = nautilus::engine::CallableFunction<void, Nautilus::Interface::HashMap*>;
    void setNautilusCleanupExec(std::shared_ptr<NautilusCleanupExec> nautilusCleanupExec, const QueryCompilation::JoinBuildSideType& buildSide);
    std::shared_ptr<NautilusCleanupExec> getNautilusCleanupExec(const QueryCompilation::JoinBuildSideType& buildSide) const;

private:
    /// shared_ptr as multiple slices need access to it
    std::shared_ptr<NautilusCleanupExec> leftCleanupStateNautilusFunction;
    std::shared_ptr<NautilusCleanupExec> rightCleanupStateNautilusFunction;

    void emitSliceIdsToProbe(
        Slice& sliceLeft,
        Slice& sliceRight,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        PipelineExecutionContext* pipelineCtx) override;
};

}
