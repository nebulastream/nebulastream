#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_PARTITIONEDSLICESTORE_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_PARTITIONEDSLICESTORE_HPP_
#include <Compiler/Exceptions/CompilerException.hpp>
#include <Exceptions/WindowProcessingException.hpp>
#include <Windowing/Experimental/KeyedSlice.hpp>
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
#include <Windowing/Experimental/SeqenceLog.hpp>
#include <assert.h>
namespace NES::Experimental {

/**
 * @brief A slice store, which stores slices in a pre-allocated array.
 * If to manny slices are inserted the slice store wraps and starts over from the beginning to reduce the number of allocations.
 * @tparam Key
 * @tparam Value
 * @tparam numberOfSlices
 */
template<class Key, class Value, uint64_t numberOfSlices = 500>
class PartitionedSliceStore {
    using KeyedSlicePtr = std::unique_ptr<PartitionedKeyedSlice<Key, Value, 1>>;

  public:
    explicit PartitionedSliceStore(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager, uint64_t sliceSize)
        : bufferManager(bufferManager), sliceSize(sliceSize) {
        for (uint64_t i = 0; i < numberOfSlices; i++) {
            slices[i] = std::make_unique<PartitionedKeyedSlice<uint64_t, uint64_t, 1>>(bufferManager, 0, 0, 0);
        }
    };

    inline KeyedSlicePtr& findSliceByTs(uint64_t ts) {
        auto logicalSliceIndex = ts / sliceSize;
        return getSlice(logicalSliceIndex);
    }

    inline const KeyedSlicePtr& first() { return slices[getArrayIndex(firstIndex)]; }

    inline const KeyedSlicePtr& operator[](uint64_t sliceIndex) { return getSlice(sliceIndex); }

    inline uint64_t getFirstIndex() { return firstIndex; }

    inline uint64_t getLastIndex() { return lastIndex - 1; }

    inline const KeyedSlicePtr& last() { return slices[getArrayIndex(getLastIndex())]; }

    std::array<KeyedSlicePtr, numberOfSlices>& getSlices() { return slices; }

    inline void dropFirstSlice() {
        // if the first index reaches the last index, we create a dummy slice at the end.
        if (firstIndex + 1 == lastIndex) {
            appendNextSlice();
        }
        this->firstIndex++;
    }

    uint64_t lastThreadLocalWatermark = 0;

  private:
    /**
     * @brief Appends a new slice to the end of the slice store.
     * @throws WindowProcessingException if the slice store is full
     */
    void appendNextSlice() {
        // calculate next slice index
        auto nextSliceIndex = getArrayIndex(lastIndex);

        // currently, we can't handle if we reached the first index
        if (getArrayIndex(firstIndex) == nextSliceIndex) {
            throw WindowProcessingException("We cant insert at this position is before "
                                            + std::to_string(getArrayIndex(firstIndex)) + " vs "
                                            + std::to_string(nextSliceIndex));
        }
        // slices are always pre-initialized. Thus, we can just call reset.
        slices[nextSliceIndex]->reset(last()->end, last()->end + sliceSize, lastIndex);
        this->lastIndex++;
    }

    /**
     * @brief Get a specific slice at a specific slice index.
     * @param sliceIndex
     * @return KeyedSlicePtr
     */
    KeyedSlicePtr& getSlice(uint64_t sliceIndex) {
        // create the first slice if non is existing [0 - slice size]
        if (lastIndex == 0) {
            // todo handle if slice should not start at beginning
            slices[0]->reset(0, sliceSize, 0);
            lastIndex++;
        }

        if (sliceIndex < firstIndex) {
            throw WindowProcessingException("Requested slice index " + std::to_string(sliceIndex) + " is before "
                                            + std::to_string(firstIndex));
        }

        if (sliceIndex >= firstIndex + numberOfSlices) {
            throw WindowProcessingException("Requested slice index " + std::to_string(sliceIndex) + " is before "
                                            + std::to_string(firstIndex));
        }

        while (sliceIndex >= lastIndex) {
            appendNextSlice();
        }

        return slices[getArrayIndex(sliceIndex)];
    }

    uint64_t getArrayIndex(uint64_t sliceIndex) { return sliceIndex % numberOfSlices; }
    bool empty() { return lastIndex == 0; }

    std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager;
    uint64_t sliceSize;
    std::array<KeyedSlicePtr, numberOfSlices> slices{};
    uint64_t firstIndex = 0;
    uint64_t lastIndex = 0;
};

}// namespace NES::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_PARTITIONEDSLICESTORE_HPP_
