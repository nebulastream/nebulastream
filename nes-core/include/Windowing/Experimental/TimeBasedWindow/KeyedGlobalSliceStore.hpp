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
