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
using KeyedSlicePtr = std::shared_ptr<KeyedSlice>;

class KeyedGlobalSliceStore {

  public:
    /**
     * @brief Add a new slice to the slice store
     * @param sliceIndex index of the slice
     * @param slice
     */
    std::tuple<uint64_t, uint64_t> addSlice(uint64_t sequenceNumber, uint64_t sliceIndex, KeyedSlicePtr slice);

    /**
     * @brief Looksup an individual slice by the slice index
     * @param sliceIndex index of the slice
     * @return KeyedSlicePtr
     */
    KeyedSlicePtr getSlice(uint64_t sliceIndex);

    /**
     * @brief Triggers a specific slice and garbage collects all slices
     * that are not necessary any more.
     * @param sliceIndex
     */
    void finalizeSlice(uint64_t sequenceNumber, uint64_t sliceIndex);

  private:
    std::mutex sliceStagingMutex;
    std::map<uint64_t, KeyedSlicePtr> sliceMap;
    WatermarkProcessor sliceAddSequenceLog;
    WatermarkProcessor sliceTriggerSequenceLog;
};

}// namespace NES::Windowing::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDGLOBALSLICESTORE_HPP_
