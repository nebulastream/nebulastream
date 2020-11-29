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

#ifndef NES_INCLUDE_WINDOWING_WINDOWMANAGER_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWMANAGER_HPP_
#include <Util/Logger.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

class WindowManager {

  public:
    explicit WindowManager(Windowing::WindowTypePtr windowType);

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
     * @param store the window slice store
     */
    template<class PartialAggregateType>
    inline void sliceStream(const uint64_t ts, WindowSliceStore<PartialAggregateType>* store, int64_t key) {
        NES_DEBUG("sliceStream for ts=" << ts << " key=" << key);
        // updates the maximal record ts
        // store->updateMaxTs(ts);
        // check if the slice store is empty
        if (store->empty()) {
            // set last watermark to current ts for processing time
            //            store->setLastWatermark(ts - allowedLateness);//TODO dont know if we still need it
            store->nextEdge = windowType->calculateNextWindowEnd(ts - allowedLateness);
            if (windowType->isTumblingWindow()) {
                TumblingWindow* window = dynamic_cast<TumblingWindow*>(windowType.get());
                store->appendSlice(SliceMetaData(store->nextEdge - window->getSize().getTime(), store->nextEdge));
                NES_DEBUG("WindowManager: for TumblingWindow sliceStream empty store, set ts as LastWatermark, startTs="
                          << store->nextEdge - window->getSize().getTime() << " nextWindowEnd=" << store->nextEdge
                          << " key=" << key);
            } else if (windowType->isSlidingWindow()) {
                SlidingWindow* window = dynamic_cast<SlidingWindow*>(windowType.get());
                store->appendSlice(SliceMetaData(store->nextEdge - window->getSlide().getTime(), store->nextEdge));
                NES_DEBUG("WindowManager: for SlidingWindow  sliceStream empty store, set ts as LastWatermark, startTs="
                          << store->nextEdge - store->nextEdge - window->getSlide().getTime()
                          << " nextWindowEnd=" << store->nextEdge << " key=" << key);
            } else {
                NES_THROW_RUNTIME_ERROR("WindowManager: Undefined Window Type");
            }
        }
        NES_DEBUG("WindowManager: sliceStream check store-nextEdge=" << store->nextEdge << " <="
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
            NES_DEBUG("append new slide for start=" << newStart << " end=" << nextEdge << " key=" << key);
            store->nextEdge = nextEdge;
            store->appendSlice(SliceMetaData(newStart, nextEdge));
        }
    }

  private:
    const uint64_t allowedLateness;
    Windowing::WindowTypePtr windowType;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWMANAGER_HPP_
