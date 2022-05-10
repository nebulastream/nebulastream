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

#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDGLOBALSLICESTORE_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDGLOBALSLICESTORE_HPP_
#include <Exceptions/WindowProcessingException.hpp>
#include <Windowing/Experimental/SeqenceLog.hpp>
#include <Windowing/Watermark/WatermarkProcessor.hpp>
#include <cinttypes>
#include <list>
#include <map>
#include <memory>
#include <mutex>
namespace NES::Windowing::Experimental {
class KeyedSlice;
using KeyedSliceSharedPtr = std::shared_ptr<KeyedSlice>;

struct Window {
    uint64_t startTs;
    uint64_t endTs;
    uint64_t sequenceNumber;
};

/**
 * @brief The global slice store that contains the final slice.
 */
class KeyedGlobalSliceStore {
  public:
    ~KeyedGlobalSliceStore();

    /**
     * @brief Add a new slice to the slice store and trigger all windows, which trigger, because we added this slice.
     * @param sequenceNumber index of the slice
     * @param slice the slice, which we want to add to the slice store
     * @param windowSize the window size
     * @param windowSlide the window slide
     * @return std::vector<Window>
     */
    std::vector<Window>
    addSliceAndTriggerWindows(uint64_t sequenceNumber, KeyedSliceSharedPtr slice, uint64_t windowSize, uint64_t windowSlide);

    /**
     * @brief Trigger all inflight window, which are covered by at least one slice in the the slice store
     * @param windowSize the window size
     * @param windowSlide the window slide
     * @return std::vector<Window>
     */
    std::vector<Window> triggerAllInflightWindows(uint64_t windowSize, uint64_t windowSlide);

    /**
     * @brief Triggers a specific slice and garbage collects all slices
     * that are not necessary any more.
     * @param sliceIndex
     */
    void finalizeSlice(uint64_t sequenceNumber, uint64_t sliceIndex);

    /**
     * @brief Returns a list of slices, which can be used to access a single slice.
     * @param startTs window start
     * @param endTs window end
     * @return std::vector<KeyedSliceSharedPtr>
     */
    std::vector<KeyedSliceSharedPtr> getSlicesForWindow(uint64_t startTs, uint64_t endTs);

  private:
    std::vector<Window> triggerInflightWindows(uint64_t windowSize, uint64_t windowSlide, uint64_t startEndTs, uint64_t endEndTs);
    std::mutex sliceStagingMutex;
    std::list<KeyedSliceSharedPtr> slices;
    WatermarkProcessor sliceAddSequenceLog;
    WatermarkProcessor sliceTriggerSequenceLog;
    uint64_t lastWindowStart = UINT64_MAX;
    uint64_t emittedWindows = 0;

};

}// namespace NES::Windowing::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDGLOBALSLICESTORE_HPP_
