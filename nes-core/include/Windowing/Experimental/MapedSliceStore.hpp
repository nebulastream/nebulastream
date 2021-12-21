#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_MAPEDSLICESTORE_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_MAPEDSLICESTORE_HPP_
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
class MapedSliceStore {
    using KeyedSlicePtr = std::unique_ptr<PartitionedKeyedSlice<Key, Value, 1>>;

  public:
    explicit MapedSliceStore(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager, uint64_t sliceSize)
        : bufferManager(bufferManager), sliceSize(sliceSize) {
        for (uint64_t i = 0; i < numberOfSlices; i++) {
            sliceStack.emplace_back(std::make_unique<PartitionedKeyedSlice<uint64_t, uint64_t, 1>>(bufferManager, 0, 0, 0));
        }
    };

    inline KeyedSlicePtr& findSliceByTs(uint64_t ts) {
        auto logicalSliceIndex = ts / sliceSize;
        return getSlice(logicalSliceIndex);
    }

    bool contains(uint64_t sliceIndex){
        return slices.contains(sliceIndex);
    }

    inline const KeyedSlicePtr& operator[](uint64_t sliceIndex) { return getSlice(sliceIndex); }

    inline uint64_t getFirstIndex() { return firstIndex; }

    inline uint64_t getLastIndex() { return lastIndex - 1; }

    inline void dropFirstSlice() {
        // if the first index reaches the last index, we create a dummy slice at the end.
        if (slices.contains(firstIndex)) {
            sliceStack.emplace_back(std::move(slices[firstIndex]));
            slices.erase(firstIndex);
        }
        this->firstIndex++;
    }

    uint64_t lastThreadLocalWatermark = 0;

  private:
    /**
     * @brief Appends a new slice to the end of the slice store.
     * @throws WindowProcessingException if the slice store is full
     */
    void insertSlice(uint64_t sliceIndex) {
        // slices are always pre-initialized. Thus, we can just call reset.
        auto startTs = sliceIndex * sliceSize;
        auto endTs = (sliceIndex + 1) * sliceSize;
        if (!sliceStack.empty()) {
            auto slice = std::move(sliceStack.back());
            sliceStack.pop_back();
            slice->reset(startTs, endTs, sliceIndex);
            slices[sliceIndex] = std::move(slice);
        } else {
            slices[sliceIndex] =
                std::make_unique<PartitionedKeyedSlice<uint64_t, uint64_t, 1>>(bufferManager, startTs, endTs, sliceIndex);
        }
    }

    /**
     * @brief Get a specific slice at a specific slice index.
     * @param sliceIndex
     * @return KeyedSlicePtr
     */
    KeyedSlicePtr& getSlice(uint64_t sliceIndex) {
        if (sliceIndex < firstIndex) {
            throw WindowProcessingException("Requested slice index " + std::to_string(sliceIndex) + " is before "
                                            + std::to_string(firstIndex));
        }

        if (!slices.contains(sliceIndex)) {
            insertSlice(sliceIndex);
            if (sliceIndex > lastIndex) {
                lastIndex = sliceIndex;
            }
        }

        return slices[sliceIndex];
    }



    std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager;
    uint64_t sliceSize;
    std::map<uint64_t, KeyedSlicePtr> slices;
    std::list<KeyedSlicePtr> sliceStack;
    uint64_t firstIndex = 0;
    uint64_t lastIndex = 0;
};

}// namespace NES::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_MAPEDSLICESTORE_HPP_
