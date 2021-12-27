#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDTHREADLOCALSLICESTORE_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDTHREADLOCALSLICESTORE_HPP_
#include <Exceptions/WindowProcessingException.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
#include <list>
#include <map>
#include <memory>

namespace NES::Experimental {
class HashMapFactory;
using HashMapFactoryPtr = std::shared_ptr<HashMapFactory>;
}// namespace NES::Experimental

namespace NES::Windowing::Experimental {

class KeyedSlice;
using KeyedSlicePtr = std::unique_ptr<KeyedSlice>;

/**
 * @brief A Slice store, which stores slices for a specific thread.
 */
class KeyedThreadLocalSliceStore {
  public:
    explicit KeyedThreadLocalSliceStore(NES::Experimental::HashMapFactoryPtr hashMapFactory,
                                        uint64_t sliceSize,
                                        uint64_t numberOfPreallocatedSlices);

    inline KeyedSlicePtr& findSliceByTs(uint64_t ts) {
        auto logicalSliceIndex = ts / sliceSize;
        return getSlice(logicalSliceIndex);
    }

    inline const KeyedSlicePtr& operator[](uint64_t sliceIndex) { return getSlice(sliceIndex); }

    inline uint64_t getFirstIndex() { return firstIndex; }

    inline uint64_t getLastIndex() { return lastIndex; }

    void dropFirstSlice();
    uint64_t getLastWatermark();
    void setLastWatermark(uint64_t watermarkTs);
  private:
    /**
     * @brief Appends a new slice to the end of the slice store.
     * @throws WindowProcessingException if the slice store is full
     */
    KeyedSlicePtr& insertSlice(uint64_t sliceIndex);

    /**
     * @brief Get a specific slice at a specific slice index.
     * @param sliceIndex
     * @return KeyedSlicePtr
     */
    inline KeyedSlicePtr& getSlice(uint64_t sliceIndex) {
        if (sliceIndex < firstIndex) {
            throw WindowProcessingException("Requested slice index " + std::to_string(sliceIndex) + " is before "
                                            + std::to_string(firstIndex));
        }

        if (currentSlice != nullptr && sliceIndex == lastIndex) {
            return currentSlice;
        } else if (!slices.contains(sliceIndex)) {
            return insertSlice(sliceIndex);
        } else {
            return slices[sliceIndex];
        }
    }

    KeyedSlicePtr allocateNewSlice(uint64_t startTs, uint64_t endTs, uint64_t sliceIndex);

  private:
    NES::Experimental::HashMapFactoryPtr hashMapFactory;
    uint64_t sliceSize;
    KeyedSlicePtr currentSlice;
    std::map<uint64_t, KeyedSlicePtr> slices;
    std::list<KeyedSlicePtr> preallocatedSlices;
    uint64_t firstIndex = 0;
    uint64_t lastIndex = 0;
    uint64_t lastWatermarkTs = 0;
};
}// namespace NES::Windowing::Experimental
#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDTHREADLOCALSLICESTORE_HPP_
