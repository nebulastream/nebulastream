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
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <HashMapSlice.hpp>


namespace NES
{

/// This task models the information for a hash join based window trigger
struct EmittedHJWindowTrigger
{
    WindowInfo windowInfo;
    uint64_t leftNumberOfHashMaps;
    uint64_t rightNumberOfHashMaps;
    Nautilus::Interface::HashMap**
        leftHashMaps; /// Pointer to the stored pointers of all hash maps of the left input stream that the probe should iterate over
    Nautilus::Interface::HashMap**
        rightHashMaps; /// Pointer to the stored pointers of all hash maps of the right input stream that the probe should iterate over
};


class HJOperatorHandler final : public StreamJoinOperatorHandler
{
public:
    HJOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        const OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
        : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
    {
    }

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const override;

    void setNautilusCleanupExec(
        std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> nautilusCleanupExec, const JoinBuildSideType& buildSide);
    [[nodiscard]] std::vector<std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec>> getNautilusCleanupExec() const;

private:
    /// shared_ptr as multiple slices need access to it
    std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> leftCleanupStateNautilusFunction;
    std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> rightCleanupStateNautilusFunction;

    void emitSlicesToProbe(
        Slice& sliceLeft,
        Slice& sliceRight,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        PipelineExecutionContext* pipelineCtx) override;
};

}
