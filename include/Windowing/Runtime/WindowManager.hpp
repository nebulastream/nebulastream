#ifndef NES_INCLUDE_WINDOWING_WINDOWMANAGER_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWMANAGER_HPP_
#include <Util/Logger.hpp>
#include <Windowing/WindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES {

class WindowManager {

  public:
    explicit WindowManager(WindowDefinitionPtr windowDefinition);

    /**
     * @brief Get the window definition for the window manager
     * @return WindowDefinition
     */
    WindowDefinitionPtr getWindowDefinition();

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
    inline void sliceStream(const uint64_t ts, WindowSliceStore<PartialAggregateType>* store) {
        NES_DEBUG("sliceStream for ts=" << ts);
        // updates the maximal record ts
        // store->updateMaxTs(ts);
        // check if the slice store is empty
        if (store->empty()) {
            // set last watermark to current ts for processing time
            store->setLastWatermark(ts - allowedLateness);
            auto windowType = windowDefinition->getWindowType();
            store->nextEdge = windowDefinition->getWindowType()->calculateNextWindowEnd(ts - allowedLateness);
            if (windowType->isTumblingWindow()) {
                TumblingWindow* window = dynamic_cast<TumblingWindow*>(windowType.get());
                store->appendSlice(SliceMetaData(store->nextEdge - window->getSize().getTime(), store->nextEdge));
                NES_TRACE("WindowManager: for TumblingWindow sliceStream empty store, set ts as LastWatermark, startTs=" << store->nextEdge - window->getSize().getTime() << " nextWindowEnd=" << store->nextEdge);
            } else if (windowType->isSlidingWindow()) {
                SlidingWindow* window = dynamic_cast<SlidingWindow*>(windowType.get());
                NES_TRACE("WindowManager: successful cast to Sliding Window fir sliceStream empty store");
                store->appendSlice(SliceMetaData(store->nextEdge - window->getSlide().getTime(), store->nextEdge));
                NES_TRACE("WindowManager: for SlidingWindow  sliceStream empty store, set ts as LastWatermark, startTs=" << store->nextEdge - store->nextEdge - window->getSlide().getTime() << " nextWindowEnd=" << store->nextEdge);
            } else {
                NES_THROW_RUNTIME_ERROR("WindowManager: Undefined Window Type");
            }
        }
        NES_DEBUG("WindowManager: sliceStream check store-nextEdge=" << store->nextEdge << " <="
                                                                     << " ts=" << ts);
        // append new slices if needed
        while (store->nextEdge <= ts) {
            auto currentSlice = store->getCurrentSliceIndex();
            NES_TRACE("WindowManager: sliceStream currentSlice=" << currentSlice);
            auto& sliceMetaData = store->getSliceMetadata();
            auto newStart = sliceMetaData[currentSlice].getEndTs();
            NES_TRACE("WindowManager: sliceStream newStart=" << newStart);
            auto nextEdge = windowDefinition->getWindowType()->calculateNextWindowEnd(store->nextEdge);
            NES_TRACE("WindowManager: sliceStream nextEdge=" << nextEdge);
            store->nextEdge = nextEdge;
            store->appendSlice(SliceMetaData(newStart, nextEdge));
        }
    }

  private:
    const uint64_t allowedLateness;
    WindowDefinitionPtr windowDefinition;
};

}// namespace NES

#endif//NES_INCLUDE_WINDOWING_WINDOWMANAGER_HPP_
