#ifndef NES_INCLUDE_WINDOWING_WINDOWSLICESTORE_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWSLICESTORE_HPP_
#include <vector>
#include <Windowing/Runtime/SliceMetaData.hpp>
#include <Util/Logger.hpp>
namespace NES{

/**
 * The WindowSliceStore stores slices consisting of metadata and a partial aggregate.
 * @tparam PartialAggregateType type of the partial aggregate
 */
template<class PartialAggregateType>
class WindowSliceStore {
  public:
    WindowSliceStore(PartialAggregateType value) : defaultValue(value),
                                                   sliceMetaData(std::vector<SliceMetaData>()),
                                                   partialAggregates(std::vector<PartialAggregateType>()) {
    }

    /**
    * Get the corresponding slide index for a particular timestamp ts.
    * If no corresponding slice exist we throw a exception.
    * @param ts timestamp of the record.
    * @return the index of a slice.
    */
    inline uint64_t getSliceIndexByTs(const uint64_t ts) {
        for (uint64_t i = 0; i < sliceMetaData.size(); i++) {
            auto slice = sliceMetaData[i];
            if (slice.getStartTs() <= ts && slice.getEndTs() > ts) {
                NES_DEBUG("getSliceIndexByTs for ts=" << ts << " return index=" << i);
                return i;
            }
        }
        return -1;
    }

    /**
     * Appends a new slice to the meta data vector and intitalises a new partial aggregate with the default value.
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
    inline uint64_t getCurrentSliceIndex() {
        return sliceMetaData.size() - 1;
    }

    inline void cleanupToPos(uint64_t pos) {
        sliceMetaData.erase(sliceMetaData.begin(), sliceMetaData.size() > pos ? sliceMetaData.begin() + pos + 1 : sliceMetaData.end());
        partialAggregates.erase(partialAggregates.begin(), partialAggregates.size() > pos ? partialAggregates.begin() + pos + 1 : partialAggregates.end());
        NES_DEBUG("cleanupToPos size after cleanup slice=" << sliceMetaData.size() << " aggs=" << partialAggregates.size());
    }

    inline uint64_t empty() {
        return sliceMetaData.empty();
    }

    inline std::vector<SliceMetaData>& getSliceMetadata() {
        return sliceMetaData;
    }

    inline std::vector<PartialAggregateType>& getPartialAggregates() {
        return partialAggregates;
    }
    uint64_t getLastWatermark() const {
        return lastWatermark;
    }
    void setLastWatermark(uint64_t last_watermark) {
        lastWatermark = last_watermark;
    }

    uint32_t getMaxTs(uint32_t originId) {
        return originIdToMaxTsMap[originId];
    };

    uint64_t getNumberOfMappings() {
        return originIdToMaxTsMap.size();
    };

    uint64_t getMinWatermark() {
        if (originIdToMaxTsMap.empty()) {
            NES_DEBUG("getMinWatermark() return 0 because there is no mapping yet");
            return 0;//TODO: we have to figure out how many downstream positions are there
        }
        std::map<uint64_t, uint64_t>::iterator min = std::min_element(originIdToMaxTsMap.begin(), originIdToMaxTsMap.end(), [](const std::pair<uint64_t, uint64_t>& a, const std::pair<uint64_t, uint64_t>& b) -> bool {
          return a.second < b.second;
        });
        NES_DEBUG("getMinWatermark() return min =" << min->second);
        return min->second;
    };

    void updateMaxTs(uint64_t ts, uint64_t originId) {
        originIdToMaxTsMap[originId] = std::max(originIdToMaxTsMap[originId], ts);
    };

    const PartialAggregateType defaultValue;
    uint64_t nextEdge = 0;

  private:
    std::vector<SliceMetaData> sliceMetaData;
    std::vector<PartialAggregateType> partialAggregates;
    uint64_t lastWatermark = 0;
    std::map<uint64_t, uint64_t> originIdToMaxTsMap;
};

}

#endif//NES_INCLUDE_WINDOWING_WINDOWSLICESTORE_HPP_
