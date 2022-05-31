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

#include <Exceptions/WindowProcessingException.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSliceStaging.hpp>

namespace NES::Windowing::Experimental {

std::tuple<uint64_t, uint64_t> KeyedSliceStaging::addToSlice(uint64_t sliceEndTs,
                                                             std::unique_ptr<std::vector<Runtime::TupleBuffer>> entries) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    if (!slicePartitionMap.contains(sliceEndTs)) {
        slicePartitionMap[sliceEndTs] = std::make_unique<Partition>(++sliceIndex);
    }
    auto& partition = slicePartitionMap[sliceEndTs];
    for (auto& entryBuffer : (*entries.get())) {
        partition->buffers.emplace_back(entryBuffer);
    }
    partition->addedSlices++;

    return {partition->addedSlices, partition->buffers.size()};
}

std::unique_ptr<KeyedSliceStaging::Partition> KeyedSliceStaging::erasePartition(uint64_t sliceEndTs) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    if (!slicePartitionMap.contains(sliceEndTs)) {
        throw WindowProcessingException("Slice Index " + std::to_string(sliceEndTs) + "not available");
    }
    auto value = std::move(slicePartitionMap[sliceEndTs]);
    auto iter = slicePartitionMap.find(sliceEndTs);
    slicePartitionMap.erase(iter);
    return value;
}

void KeyedSliceStaging::clear() {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    slicePartitionMap.clear();
}

}// namespace NES::Windowing::Experimental