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

#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEWINDOW_GLOBALSLICESTAGING_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEWINDOW_GLOBALSLICESTAGING_HPP_

#include <Windowing/Experimental/GlobalTimeWindow/GlobalSlice.hpp>
#include <cinttypes>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace NES::Windowing::Experimental {

class State;

/**
 * @brief The slice staging area is used as an area for the merging of the local slices.
 * Whenever a thread local slice store received a watermark it is assigning all slices that end before the particular slice to the stating area.
 * As multiple threads can concurrently append slices, we synchronize accesses.
 */
class GlobalSliceStaging {
  public:
    /**
     * @brief Stores the partitions for a specific slice. For global windows,
     * this is storing the single thread local aggregation values.
     */
    class Partition {
      public:
        Partition(uint64_t sliceIndex) : sliceIndex(sliceIndex) {}
        std::vector<std::unique_ptr<State>> partialStates;
        uint64_t addedSlices = 0;
        const uint64_t sliceIndex;
    };

    /**
     * @brief Appends the state of a slice to the staging area.
     * @param sliceEndTs we use the slice endTs as an index for the map of slices
     * @param entries the entries of the slice.
     * @return returns the number of threads already appended a slice to the staging area.
     */
    std::tuple<uint64_t, uint64_t> addToSlice(uint64_t sliceEndTs, std::unique_ptr<State> state);

    /**
     * @brief Extracts a partition from the staging area.
     * @param sliceEndTs
     * @return
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
}// namespace NES::Windowing::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEWINDOW_GLOBALSLICESTAGING_HPP_
