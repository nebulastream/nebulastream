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

#ifndef NES_INCLUDE_WINDOWING_RUNTIME_WINDOW_MANAGER_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_WINDOW_MANAGER_HPP_
#include <Util/Logger.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Runtime/SliceMetaData.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

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
        NES_DEBUG("WindowManager store" << id << ": sliceStream for ts=" << ts << " key=" << key
                                        << " allowedLateness=" << allowedLateness);
        // updates the maximal record ts
        // check if the slice store is empty or if the first slice has a larger ts then the current event.
        if (store->empty() || store->getSliceMetadata().front().getStartTs() > ts) {
            // set last watermark to current ts for processing time
            if (ts < allowedLateness) {
                store->nextEdge = windowType->calculateNextWindowEnd(0);
            } else {
                store->nextEdge = windowType->calculateNextWindowEnd(ts - allowedLateness);
            }

            if (windowType->isTumblingWindow()) {
                auto* window = dynamic_cast<TumblingWindow*>(windowType.get());
                store->prependSlice(SliceMetaData(store->nextEdge - window->getSize().getTime(), store->nextEdge));
                NES_DEBUG("WindowManager " << id
                                           << ": for TumblingWindow sliceStream empty store, set ts as LastWatermark, startTs="
                                           << store->nextEdge - window->getSize().getTime()
                                           << " nextWindowEnd=" << store->nextEdge << " key=" << key);
            } else if (windowType->isSlidingWindow()) {
                auto* window = dynamic_cast<SlidingWindow*>(windowType.get());
                store->prependSlice(SliceMetaData(store->nextEdge - window->getSlide().getTime(), store->nextEdge));
                NES_DEBUG("WindowManager " << id
                                           << ": for SlidingWindow  sliceStream empty store, set ts as LastWatermark, startTs="
                                           << store->nextEdge - window->getSlide().getTime()
                                           << " nextWindowEnd=" << store->nextEdge << " key=" << key);
            } else {
                NES_THROW_RUNTIME_ERROR("WindowManager: Undefined Window Type");
            }
        }
        NES_DEBUG("WindowManager " << id << ": sliceStream check store-nextEdge=" << store->nextEdge << " <="
                                   << " ts=" << ts << " key=" << key);

        // append new slices if needed
        while (store->nextEdge <= ts) {
            auto currentSlice = store->getCurrentSliceIndex();
            NES_TRACE("WindowManager " << id << " sliceStream currentSlice=" << currentSlice << " key=" << key);
            auto& sliceMetaData = store->getSliceMetadata();
            auto newStart = sliceMetaData[currentSlice].getEndTs();
            NES_TRACE("WindowManager " << id << " sliceStream newStart=" << newStart << " key=" << key);
            auto nextEdge = windowType->calculateNextWindowEnd(store->nextEdge);
            NES_TRACE("WindowManager: sliceStream nextEdge=" << nextEdge << " key=" << key);
            NES_TRACE("WindowManager " << id << ": append new slide for start=" << newStart << " end=" << nextEdge
                                       << " key=" << key);
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
    template<class StreamType, class KeyType>
    inline void sliceStream(const uint64_t ts, WindowedJoinSliceListStore<StreamType>* store, KeyType key) {
        NES_TRACE("WindowManager list " << id << ": sliceStream for ts=" << ts << " key=" << key);
        // updates the maximal record ts
        // store->updateMaxTs(ts);
        // check if the slice store is empty
        if (store->empty()) {
            // set last watermark to current ts for processing time
            store->nextEdge = windowType->calculateNextWindowEnd(ts - allowedLateness);
            if (windowType->isTumblingWindow()) {
                auto* window = dynamic_cast<TumblingWindow*>(windowType.get());
                store->appendSlice(SliceMetaData(store->nextEdge - window->getSize().getTime(), store->nextEdge));
                NES_TRACE("WindowManager " << id
                                           << ": for TumblingWindow sliceStream empty store, set ts as LastWatermark, startTs="
                                           << store->nextEdge - window->getSize().getTime()
                                           << " nextWindowEnd=" << store->nextEdge << " key=" << key);
            } else if (windowType->isSlidingWindow()) {
                auto* window = dynamic_cast<SlidingWindow*>(windowType.get());
                store->appendSlice(SliceMetaData(store->nextEdge - window->getSlide().getTime(), store->nextEdge));
                NES_TRACE("WindowManager list "
                          << id << ": for SlidingWindow  sliceStream empty store, set ts as LastWatermark, startTs="
                          << store->nextEdge - window->getSlide().getTime() << " nextWindowEnd=" << store->nextEdge
                          << " key=" << key);
            } else {
                NES_THROW_RUNTIME_ERROR("WindowManager: Undefined Window Type");
            }
        }
        NES_TRACE("WindowManager list " << id << ": sliceStream check store-nextEdge=" << store->nextEdge << " <="
                                        << " ts=" << ts << " key=" << key);

        // append new slices if needed
        while (store->nextEdge <= ts) {
            auto currentSlice = store->getCurrentSliceIndex();
            NES_TRACE("WindowManager: sliceStream currentSlice=" << currentSlice << " key=" << key);
            auto& sliceMetaData = store->getSliceMetadata();
            auto newStart = sliceMetaData[currentSlice].getEndTs();
            NES_TRACE("WindowManager: sliceStream newStart=" << newStart << " key=" << key);
            auto nextEdge = windowType->calculateNextWindowEnd(store->nextEdge);
            NES_TRACE("WindowManager: sliceStream nextEdge=" << nextEdge << " key=" << key);
            NES_TRACE("WindowManager list " << id << ": append new slide for start=" << newStart << " end=" << nextEdge
                                            << " key=" << key);
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

#endif  // NES_INCLUDE_WINDOWING_RUNTIME_WINDOW_MANAGER_HPP_
