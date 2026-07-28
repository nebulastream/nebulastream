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
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <HashMapSlice.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

class SampleOperatorHandler final : public WindowBasedOperatorHandler
{
public:
    SampleOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        uint64_t valueSize,
        uint64_t pageSize);

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

private:
    static constexpr uint64_t NUMBER_OF_BUCKETS = 1;
    uint64_t valueSize;
    uint64_t pageSize;
};

}
