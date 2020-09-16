#include <API/AbstractWindowDefinition.hpp>
#include <API/Window/TimeCharacteristic.hpp>
#include <State/StateVariable.hpp>
#include <Util/Logger.hpp>
#include <memory>
#include <utility>
#include <vector>
#ifndef WINDOWMANAGERLIB_HPP
#define WINDOWMANAGERLIB_HPP

namespace NES {

/**
* SliceMetaData stores the meta data of a slice to identify if a record can be assigned to a particular slice.
*/
class SliceMetaData {
  public:
    SliceMetaData(uint64_t start_ts, uint64_t end_ts) : start_ts(start_ts), end_ts(end_ts) {}

    /**
    * The start timestamp of this slice
    */
    uint64_t getStartTs() {
        return start_ts;
    }

    /**
     * The end timestamp of this slice
     */
    uint64_t getEndTs() {
        return end_ts;
    }

  private:
    uint64_t start_ts;
    uint64_t end_ts;
};

inline uint64_t getTsFromClock() {
    return time(NULL) * 1000;
}

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

class WindowManager {

  public:
    WindowManager(WindowDefinitionPtr windowDefinition)
        : windowDefinition(std::move(windowDefinition)), allowedLateness(0) {}

    /**
     * Creates slices for in the window slice store if needed.
     * @tparam PartialAggregateType
     * @param ts current record timestamp for which a slice should exist
     * @param store the window slice store
     */
    template<class PartialAggregateType>
    inline void sliceStream(const uint64_t ts, WindowSliceStore<PartialAggregateType>* store) {

        NES_DEBUG("sliceStream for ts=" << ts);
        // updates the maximal record ts
        //        store->updateMaxTs(ts);//TODO I am not use if we still need this

        // check if the slice store is empty
        if (store->empty()) {
            // set last watermark to current ts for processing time
            store->setLastWatermark(ts - allowedLateness);//TODO: I am not sure if we still need this

            store->nextEdge = windowDefinition->getWindowType()->calculateNextWindowEnd(ts - allowedLateness);
            store->appendSlice(SliceMetaData(store->nextEdge - windowDefinition->getWindowType()->getTime(), store->nextEdge));
            NES_DEBUG("sliceStream empty store, set ts as LastWatermark, startTs=" << store->nextEdge - windowDefinition->getWindowType()->getTime() << " nextWindowEnd=" << store->nextEdge);
        }

        NES_DEBUG("sliceStream check store-nextEdge=" << store->nextEdge << " <="
                                                      << " ts=" << ts);
        // append new slices if needed
        while (store->nextEdge <= ts) {
            auto currentSlice = store->getCurrentSliceIndex();
            NES_DEBUG("sliceStream currentSlice=" << currentSlice);
            auto& sliceMetaData = store->getSliceMetadata();
            auto newStart = sliceMetaData[currentSlice].getEndTs();
            NES_DEBUG("sliceStream newStart=" << sliceMetaData[currentSlice].getEndTs());
            auto nextEdge = windowDefinition->getWindowType()->calculateNextWindowEnd(ts);
            NES_DEBUG("sliceStream nextEdge=" << windowDefinition->getWindowType()->calculateNextWindowEnd(ts));
            store->nextEdge = nextEdge;
            store->appendSlice(SliceMetaData(newStart, nextEdge));
        }
    }

  private:
    WindowDefinitionPtr windowDefinition;

  public:
    const WindowDefinitionPtr& GetWindowDefinition() const {
        return windowDefinition;
    }
    uint64_t GetAllowedLateness() const {
        return allowedLateness;
    }

  private:
    const uint64_t allowedLateness;
};

typedef std::shared_ptr<WindowManager> WindowManagerPtr;
}// namespace NES
#endif//WINDOWMANAGERLIB_HPP
