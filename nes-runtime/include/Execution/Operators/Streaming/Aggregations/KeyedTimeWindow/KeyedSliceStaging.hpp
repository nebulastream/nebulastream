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

#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDSLICESTAGING_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDSLICESTAGING_HPP_

#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <cinttypes>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace NES::Nautilus::Interface {
class ChainedHashMap;
}
namespace NES::Runtime::Execution::Operators {

class State;
class KeyedSlice;

/**
 * @brief The slice staging area is used as an area for the merging of the local slices.
 * Whenever a thread local slice store received a watermark it is assigning all slices that end before the particular watermark to the staging area.
 * As multiple threads can concurrently append slices, we synchronize accesses with a lock.
 */
class KeyedSliceStaging {
  public:
    /**
     * @brief Stores the partitions for a specific slice. For global windows,
     * this is storing the single thread local aggregation values.
     */
    class Partition {
      public:
        explicit Partition(uint64_t sliceIndex) : sliceIndex(sliceIndex) {}
        std::vector<std::unique_ptr<Nautilus::Interface::ChainedHashMap>> partialStates;
        uint64_t addedSlices = 0;
        const uint64_t sliceIndex;
    };

    /**
     * @brief Appends the state of a slice to the staging area.
     * @param sliceEndTs we use the slice endTs as an index for the map of slices
     * @param entries the entries of the slice.
     * @return returns the number of threads already appended a slice to the staging area.
     */
    std::tuple<uint64_t, uint64_t> addToSlice(uint64_t sliceEndTs,
                                              std::unique_ptr<Nautilus::Interface::ChainedHashMap> sliceState);

    /**
     * @brief Extracts a partition from the staging area and removes it.
     * @param sliceEndTs
     * @return std::unique_ptr<Partition>
     */
    std::unique_ptr<Partition> erasePartition(uint64_t sliceEndTs);

    /**
     * @brief Clears all elements in the staging area.
     */
    void clear();

  private:
    std::mutex sliceStagingMutex;
    uint64_t sliceIndex;
    std::map<uint64_t, std::unique_ptr<Partition>> slicePartitionMap;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDSLICESTAGING_HPP_
