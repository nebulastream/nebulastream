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
#include <bit>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/RollingAverage.hpp>
#include <HashMapSlice.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// This struct models the information for an aggregation window trigger
/// As we are triggering the probe pipeline by passing a tuple buffer to the probe operator, we assume that the tuple buffer
/// is large enough to store all slices of the window to be triggered.
struct EmittedAggregationWindow
{
    EmittedAggregationWindow(const WindowInfo windowInfo, std::unique_ptr<HashMap> finalHashMap, const std::vector<HashMap*>& allHashMaps)
        : windowInfo(windowInfo), finalHashMap(std::move(finalHashMap)), numberOfHashMaps(allHashMaps.size())
    {
        finalHashMapPtr = this->finalHashMap.get();
        /// Copying the hashmap pointers after this object, hence this + 1
        hashMaps = std::bit_cast<HashMap**>(this + 1);
        std::ranges::copy(allHashMaps, std::bit_cast<HashMap**>(hashMaps));
    }

    WindowInfo windowInfo;
    HashMap* finalHashMapPtr;
    std::unique_ptr<HashMap> finalHashMap; /// Pointer to the final hash map that the probe should use to combine all hash maps
    uint64_t numberOfHashMaps;
    HashMap** hashMaps; /// Pointer to the stored pointers of all hash maps that the probe should combine
};

class AggregationOperatorHandler final : public WindowBasedOperatorHandler
{
public:
    AggregationOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        uint64_t maxNumberOfBuckets);

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const override;

    /// Is required to not perform the setup again and resolving a race condition to the cleanup state function
    std::atomic<bool> setupAlreadyCalled;
    /// shared_ptr as multiple slices need access to it
    std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> cleanupStateNautilusFunction;

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;
    folly::Synchronized<RollingAverage<uint64_t>> rollingAverageNumberOfKeys;
    uint64_t maxNumberOfBuckets;
};

}
