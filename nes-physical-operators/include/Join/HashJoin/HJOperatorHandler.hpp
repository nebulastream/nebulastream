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
#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
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
#include <Util/RollingAverage.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

/// This task models the information for a hash join based window trigger
struct EmittedHJWindowTrigger
{
    EmittedHJWindowTrigger(
        const WindowInfo windowInfo,
        const std::vector<Nautilus::Interface::HashMap*>& leftHashMaps,
        const std::vector<Nautilus::Interface::HashMap*>& rightHashMaps)
        : windowInfo(windowInfo), leftNumberOfHashMaps(leftHashMaps.size()), rightNumberOfHashMaps(rightHashMaps.size())
    {
        /// Copying the left and right hashmap pointer pointers after this object, hence this + 1
        const auto leftHashMapPtrSizeInByte = leftHashMaps.size() * sizeof(Nautilus::Interface::HashMap*);
        auto* addressFirstLeftHashMapPtr = std::bit_cast<int8_t*>(this + 1);
        auto* addressFirstRightHashMapPtr = std::bit_cast<int8_t*>(this + 1) + leftHashMapPtrSizeInByte;
        this->leftHashMaps = std::bit_cast<Nautilus::Interface::HashMap**>(addressFirstLeftHashMapPtr);
        this->rightHashMaps = std::bit_cast<Nautilus::Interface::HashMap**>(addressFirstRightHashMapPtr);
        std::ranges::copy(leftHashMaps, std::bit_cast<Nautilus::Interface::HashMap**>(addressFirstLeftHashMapPtr));
        std::ranges::copy(rightHashMaps, std::bit_cast<Nautilus::Interface::HashMap**>(addressFirstRightHashMapPtr));
    }

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
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        uint64_t maxNumberOfBuckets);
    ~HJOperatorHandler() override;

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const override;

    bool wasSetupCalled(const JoinBuildSideType& buildSide);
    void setNautilusCleanupExec(
        std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> nautilusCleanupExec, const JoinBuildSideType& buildSide);
    [[nodiscard]] std::vector<std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec>> getNautilusCleanupExec() const;
    void requestCheckpoint(const JoinBuildSideType buildSide) const;
    bool consumeCheckpointRequest(const JoinBuildSideType buildSide) const;
    void setCheckpointCallbackId(const JoinBuildSideType buildSide, std::string callbackId);
    bool hasCheckpointCallback(const JoinBuildSideType buildSide) const;
    void clearCheckpointCallback(const JoinBuildSideType buildSide) const;
    [[nodiscard]] bool wasCheckpointRestored(const JoinBuildSideType buildSide) const;
    bool markCheckpointRestored(const JoinBuildSideType buildSide) const;

private:
    /// Is required to not perform the setup again and resolving a race condition to the cleanup state function
    std::atomic<bool> setupAlreadyCalledLeft;
    std::atomic<bool> setupAlreadyCalledRight;
    /// shared_ptr as multiple slices need access to it
    std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> leftCleanupStateNautilusFunction;
    std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> rightCleanupStateNautilusFunction;
    mutable std::atomic<bool> leftCheckpointRequested{false};
    mutable std::atomic<bool> rightCheckpointRequested{false};
    mutable std::atomic<bool> leftCheckpointRestored{false};
    mutable std::atomic<bool> rightCheckpointRestored{false};
    mutable std::optional<std::string> leftCheckpointCallbackId;
    mutable std::optional<std::string> rightCheckpointCallbackId;


    void emitSlicesToProbe(
        Slice& sliceLeft,
        Slice& sliceRight,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        PipelineExecutionContext* pipelineCtx) override;

    folly::Synchronized<RollingAverage<uint64_t>> rollingAverageNumberOfKeys;
    uint64_t maxNumberOfBuckets;
};

}
