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
#include <map>
#include <memory>
#include <mutex>
namespace NES::Windowing::Experimental {
class KeyedSlice;
using KeyedSliceSharedPtr = std::shared_ptr<KeyedSlice>;

/**
 * @brief The global slice store that contains the final slice.
 */
class KeyedGlobalSliceStore {
  public:
    /**
     * @brief Add a new slice to the slice store
     * @param sliceIndex index of the slice
     * @param slice
     */
    std::tuple<uint64_t, uint64_t> addSlice(uint64_t sequenceNumber, uint64_t sliceIndex, KeyedSliceSharedPtr slice);

    /**
     * @brief Looksup an individual slice by the slice index
     * @param sliceIndex index of the slice
     * @return KeyedSlicePtr
     */
    KeyedSliceSharedPtr getSlice(uint64_t sliceIndex);
    bool hasSlice(uint64_t sliceIndex);

    /**
     * @brief Triggers a specific slice and garbage collects all slices
     * that are not necessary any more.
     * @param sliceIndex
     */
    void finalizeSlice(uint64_t sequenceNumber, uint64_t sliceIndex);
    void clear();

  private:
    std::mutex sliceStagingMutex;
    std::map<uint64_t, KeyedSliceSharedPtr> sliceMap;
    WatermarkProcessor sliceAddSequenceLog;
    WatermarkProcessor sliceTriggerSequenceLog;
    uint64_t slicesPerWindow;
};

}// namespace NES::Windowing::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDGLOBALSLICESTORE_HPP_
