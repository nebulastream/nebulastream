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

#ifndef NES_CORE_INCLUDE_WINDOWING_RUNTIME_WINDOWMANAGER_HPP_
#define NES_CORE_INCLUDE_WINDOWING_RUNTIME_WINDOWMANAGER_HPP_
#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Windowing/Runtime/SliceMetaData.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>

namespace NES::Windowing {

class WindowManager {

  public:
    /**
     * @brief constructor of window manager
     * @param windowType type of the window
     * @param allowedLateness in ms
     * @param id of the window manager
     */
    explicit WindowManager(Windowing::WindowTypePtr windowType, uint64_t allowedLateness, uint64_t id);

    /**
     * @brief Get the window type for the window manager
     * @return WindowTypePtr
     */
    Windowing::WindowTypePtr getWindowType();

    /**
     * @brief Get the allowed lateness
     * @return Allowed lateness
     */
    [[nodiscard]] uint64_t getAllowedLateness() const;

    /**
     * Creates slices for in the window slice store if needed.
     * @tparam PartialAggregateType
     * @param ts current record timestamp for which a slice should exist
     * @param key, for debugging purposes we need the key
     * @param store the window slice store
     */
    template<class PartialAggregateType, class KeyType>
    inline void sliceStream(const uint64_t ts, WindowSliceStore<PartialAggregateType>* store, KeyType key) {
        NES_TRACE("WindowManager store {}: sliceStream for ts={} key={} allowedLateness={}", id, ts, key, allowedLateness);
        auto timeBasedWindowType = WindowType::asTimeBasedWindowType(windowType);
        // updates the maximal record ts
        // check if the slice store is empty or if the first slice has a larger ts then the current event.
        if (store->empty() || store->getSliceMetadata().front().getStartTs() > ts) {
            // set last watermark to current ts for processing time
            if (ts < allowedLateness) {
                store->nextEdge = timeBasedWindowType->calculateNextWindowEnd(0);
            } else {
                store->nextEdge = timeBasedWindowType->calculateNextWindowEnd(ts - allowedLateness);
            }

            if (timeBasedWindowType->getTimeBasedSubWindowType() == TimeBasedWindowType::TUMBLINGWINDOW) {
                auto* window = dynamic_cast<TumblingWindow*>(windowType.get());
                store->prependSlice(SliceMetaData(store->nextEdge - window->getSize().getTime(), store->nextEdge));
                NES_TRACE("WindowManager {}: for TumblingWindow sliceStream empty store, set ts as LastWatermark, startTs={} "
                          "nextWindowEnd={} key={}",
                          id,
                          store->nextEdge - window->getSize().getTime(),
                          store->nextEdge,
                          key);
            } else if (timeBasedWindowType->getTimeBasedSubWindowType() == TimeBasedWindowType::SLIDINGWINDOW) {
                auto* window = dynamic_cast<SlidingWindow*>(windowType.get());
                store->prependSlice(SliceMetaData(store->nextEdge - window->getSlide().getTime(), store->nextEdge));
                NES_TRACE("WindowManager {}: for SlidingWindow sliceStream empty store, set ts as LastWatermark, startTs={} "
                          "nextWindowEnd={} key={}",
                          id,
                          store->nextEdge - window->getSlide().getTime(),
                          store->nextEdge,
                          key);
            } else {
                NES_THROW_RUNTIME_ERROR("WindowManager: Undefined Window Type");
            }
        }
        NES_TRACE("WindowManager {}: sliceStream check store-nextEdge={} <= ts={} key={}", id, store->nextEdge, ts, key);

        // append new slices if needed
        while (store->nextEdge <= ts) {
            auto currentSlice = store->getCurrentSliceIndex();
            NES_TRACE("WindowManager {} sliceStream currentSlice={} key={}", id, currentSlice, key);
            auto& sliceMetaData = store->getSliceMetadata();
            auto newStart = sliceMetaData[currentSlice].getEndTs();
            NES_TRACE("WindowManager {} sliceStream newStart={} key={}", id, newStart, key);
            auto nextEdge = timeBasedWindowType->calculateNextWindowEnd(store->nextEdge);
            NES_TRACE("WindowManager {} sliceStream nextEdge={} key={}", id, nextEdge, key);
            NES_TRACE("WindowManager {}: append new slide for start={} end= {} key={}", id, newStart, nextEdge, key);
            store->nextEdge = nextEdge;
            store->appendSlice(SliceMetaData(newStart, nextEdge));
        }
    }

    /**
     * Creates slices for in the window slice store if needed.
     * @tparam PartialAggregateType
     * @param ts current record timestamp for which a slice should exist
     * @param key, for debugging purposes we need the key
     * @param store the window slice store
     */
    template<class SourceType, class KeyType>
    inline void sliceStream(const uint64_t ts, WindowedJoinSliceListStore<SourceType>* store, KeyType key) {
        NES_TRACE("WindowManager list {}: sliceStream for ts={} key={}", id, ts, key);
        auto timeBasedWindowType = WindowType::asTimeBasedWindowType(windowType);
        // updates the maximal record ts
        // store->updateMaxTs(ts);
        // check if the slice store is empty
        if (store->empty()) {
            // set last watermark to current ts for processing time
            store->nextEdge = timeBasedWindowType->calculateNextWindowEnd(ts - allowedLateness);
            if (timeBasedWindowType->getTimeBasedSubWindowType() == TimeBasedWindowType::TUMBLINGWINDOW) {
                auto* window = dynamic_cast<TumblingWindow*>(timeBasedWindowType.get());
                store->appendSlice(SliceMetaData(store->nextEdge - window->getSize().getTime(), store->nextEdge));
                NES_TRACE("WindowManager {}: for TumblingWindow sliceStream empty store, set ts as LastWatermark, startTs={} "
                          "nextWindowEnd={}  key={}",
                          id,
                          store->nextEdge - window->getSize().getTime(),
                          store->nextEdge,
                          key);
            } else if (timeBasedWindowType->getTimeBasedSubWindowType() == TimeBasedWindowType::SLIDINGWINDOW) {
                auto* window = dynamic_cast<SlidingWindow*>(windowType.get());
                store->appendSlice(SliceMetaData(store->nextEdge - window->getSlide().getTime(), store->nextEdge));
                NES_TRACE("WindowManager list {}: for SlidingWindow sliceStream empty store, set ts as LastWatermark, "
                          "startTs={} nextWindowEnd={}  key={}",
                          id,
                          store->nextEdge - window->getSize().getTime(),
                          store->nextEdge,
                          key);
            } else {
                NES_THROW_RUNTIME_ERROR("WindowManager: Undefined Window Type");
            }
        }
        NES_TRACE("WindowManager list {}: sliceStream check store-nextEdge={} <= ts={}  key={}", id, store->nextEdge, ts, key);

        // append new slices if needed
        while (store->nextEdge <= ts) {
            auto currentSlice = store->getCurrentSliceIndex();
            NES_TRACE("WindowManager: sliceStream currentSlice={} key={}", key, currentSlice);
            auto& sliceMetaData = store->getSliceMetadata();
            auto newStart = sliceMetaData[currentSlice].getEndTs();
            NES_TRACE("WindowManager: sliceStream newStart={} key={}", newStart, key);
            auto nextEdge = timeBasedWindowType->calculateNextWindowEnd(store->nextEdge);
            NES_TRACE("WindowManager: sliceStream nextEdge={} key={}", nextEdge, key);
            NES_TRACE("WindowManager list {}: append new slide for start={} end={} key={}", id, newStart, nextEdge, key);
            store->nextEdge = nextEdge;
            store->appendSlice(SliceMetaData(newStart, nextEdge));
        }
        (void(key));
    }

  private:
    const uint64_t allowedLateness;
    Windowing::WindowTypePtr windowType;
    uint64_t id;
};

}// namespace NES::Windowing

#endif// NES_CORE_INCLUDE_WINDOWING_RUNTIME_WINDOWMANAGER_HPP_
