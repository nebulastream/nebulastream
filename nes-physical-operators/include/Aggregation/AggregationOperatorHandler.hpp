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
#include <map>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// This struct models the information for an aggregation window trigger
/// As we are triggering the probe pipeline by passing a tuple buffer to the probe operator, we assume that the tuple buffer
/// is large enough to store all slices of the window to be triggered.
struct EmittedAggregationWindow
{
    WindowInfo windowInfo;
    std::unique_ptr<Nautilus::Interface::HashMap>
        finalHashMap; /// Pointer to the final hash map that the probe should use to combine all hash maps
    uint64_t numberOfHashMaps;
    Nautilus::Interface::HashMap** hashMaps; /// Pointer to the stored pointers of all hash maps that the probe should combine
};

class AggregationOperatorHandler final : public WindowBasedOperatorHandler
{
public:
    AggregationOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore);


    /// We do not wish to set the hash map specific params during the lowering from the logical to physical
    /// TODO #409 This might change after the [DD] Operator Representations  has been implemented
    void setHashMapParams(uint64_t keySize, uint64_t valueSize, uint64_t pageSize, uint64_t numberOfBuckets);

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)> getCreateNewSlicesFunction() const override;

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;

    uint64_t keySize{};
    uint64_t valueSize{};
    uint64_t pageSize{};
    uint64_t numberOfBuckets{};
};

}
