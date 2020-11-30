/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_WINDOWING_WINDOWSLICESTORE_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWSLICESTORE_HPP_
#include <Util/Logger.hpp>
#include <Windowing/Runtime/SliceMetaData.hpp>
namespace NES::Windowing {

/**
 * @brief The WindowSliceStore stores slices consisting of metadata and a partial aggregate.
 * @tparam PartialAggregateType type of the partial aggregate
 */
template<class PartialAggregateType>
class WindowSliceStore {
  public:
    WindowSliceStore(PartialAggregateType value)
        : defaultValue(value), sliceMetaData(std::vector<SliceMetaData>()),
          partialAggregates(std::vector<PartialAggregateType>()) {}

    /**
    * @brief Get the corresponding slide index for a particular timestamp ts.
    * @param ts timestamp of the record.
    * @return the index of a slice. If not found it returns -1.
    */
    inline uint64_t getSliceIndexByTs(const uint64_t ts) {
        for (uint64_t i = 0; i < sliceMetaData.size(); i++) {
            auto slice = sliceMetaData[i];
            if (slice.getStartTs() <= ts && slice.getEndTs() > ts) {
                NES_DEBUG("getSliceIndexByTs for ts=" << ts << " return index=" << i);
                return i;
            }
        }
        NES_ERROR("getSliceIndexByTs for ts=" << ts << " could not find a slice, this should not happen");
        return -1;
    }

    /**
     * @brief Appends a new slice to the meta data vector and intitalises a new partial aggregate with the default value.
     * @param slice
     */
    inline void appendSlice(SliceMetaData slice) {
        NES_DEBUG("appendSlice "
                  << " start=" << slice.getStartTs() << " end=" << slice.getEndTs());
        sliceMetaData.push_back(slice);
        partialAggregates.push_back(defaultValue);
    }

    /**
     * @return most current slice'index.
     */
    inline uint64_t getCurrentSliceIndex() { return sliceMetaData.size() - 1; }

    inline void incrementRecordCnt(uint64_t slideIdx) { sliceMetaData[slideIdx].incrementRecordsPerSlice(); }
    /**
     * @brief Remove slices between index 0 and pos.
     * @param pos the position till we want to remove slices.
     */
    inline void removeSlicesUntil(uint64_t pos) {
        sliceMetaData.erase(sliceMetaData.begin(),
                            sliceMetaData.size() > pos ? sliceMetaData.begin() + pos + 1 : sliceMetaData.end());
        partialAggregates.erase(partialAggregates.begin(),
                                partialAggregates.size() > pos ? partialAggregates.begin() + pos + 1 : partialAggregates.end());
        NES_DEBUG("WindowSliceStore: removeSlicesUntil size after cleanup slice=" << sliceMetaData.size()
                                                                                  << " aggs=" << partialAggregates.size());
    }

    /**
     * @brief Checks if the slice store is empty.
     * @return true if empty.
     */
    inline uint64_t empty() { return sliceMetaData.empty(); }

    /**
     * @brief Gets the slice meta data.
     * @return vector of slice meta data.
     */
    inline std::vector<SliceMetaData>& getSliceMetadata() { return sliceMetaData; }

    /**
     * @brief Gets partial aggregates.
     * @return vector of partial aggregates.
     */
    inline std::vector<PartialAggregateType>& getPartialAggregates() { return partialAggregates; }

    const PartialAggregateType defaultValue;
    uint64_t nextEdge = 0;

  private:
    std::vector<SliceMetaData> sliceMetaData;
    std::vector<PartialAggregateType> partialAggregates;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWSLICESTORE_HPP_
