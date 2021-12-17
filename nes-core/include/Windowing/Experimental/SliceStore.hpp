#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_SLICESTORE_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_SLICESTORE_HPP_

#include <Windowing/Experimental/KeyedSlice.hpp>
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
#include <Windowing/Experimental/SeqenceLog.hpp>
#include <assert.h>
namespace NES::Experimental {
// A slice store, which contains a set of slices
template<class HashMap, uint64_t numberOfSlices = 50>
class SliceStore {
    using KeyedSlicePtr = std::unique_ptr<KeyedSlice<HashMap>>;

  public:
    explicit SliceStore(uint64_t sliceSize) : sliceSize(sliceSize){};
    inline KeyedSlicePtr& findSliceByTs(uint64_t ts) {

        // create the first slice if non is existing [0 - slice size]
        if (empty()) {
            slices[0] = std::make_unique<KeyedSlice<HashMap>>(0, sliceSize);
        }

        // if the last slice ends before the current event ts we have to add new slices to the end as long es we don't cover ts.
        // if the gap between last and ts is large this could create a large number of slices.
        while (last()->end <= ts) {
            appendNextSlice();
        }

        // find the correct slice
        // calculate the slice offset by the start ts and the event ts.
        auto diffFromStart = ts - first()->start;
        auto sliceOffset = diffFromStart / sliceSize;
        auto sliceIndex = (firstIndex + sliceOffset);
        return slices[getArrayIndex(sliceIndex)];
    }
    inline KeyedSlicePtr& first() { return slices[getArrayIndex(firstIndex)]; }
    inline KeyedSlicePtr& operator[](uint64_t sliceIndex) { return slices[getArrayIndex(sliceIndex)]; }

    inline uint64_t getFirstIndex() { return firstIndex; }

    inline uint64_t getLastIndex() { return lastIndex; }

    inline KeyedSlicePtr& last() { return slices[getArrayIndex(lastIndex)]; }

    inline void eraseFirst(uint64_t n) {
        // std::cout << "erase slice at " << firstIndex << " (" << slices[getArrayIndex(firstIndex)]->start << "-"
        //         << slices[getArrayIndex(firstIndex)]->end << ")" << std::endl;
        this->firstIndex = firstIndex + n;
    }
    std::array<KeyedSlicePtr, numberOfSlices>& getSlices() { return slices; }
    uint64_t lastThreadLocalWatermark = 0;
    uint64_t outputs = 0;

  private:
    uint64_t getArrayIndex(uint64_t sliceIndex) { return sliceIndex % numberOfSlices; }
    bool empty() { return firstIndex == lastIndex; }
    void appendNextSlice() {
        // calculate next slice index
        auto nextSliceIndex = getArrayIndex(lastIndex + 1);
        // currently, we can't handle if we reached the first index
        assert(nextSliceIndex != firstIndex);
        // std::cout << "add slice at " << nextSliceIndex << std::endl;
        if (slices[nextSliceIndex] == nullptr) {
            // create a new keyed slice
            slices[nextSliceIndex] = std::make_unique<KeyedSlice<HashMap>>(last()->end, last()->end + sliceSize);
        } else {
            // re-use old slice
            slices[nextSliceIndex]->reset(last()->end, last()->end + sliceSize);
        }
        this->lastIndex++;
    }
    uint64_t sliceSize;
    std::array<KeyedSlicePtr, numberOfSlices> slices{};
    uint32_t firstIndex = 0;
    uint32_t lastIndex = 0;
};

template<class HashMap>
class GlobalSliceStore : public SliceStore<HashMap> {
  public:
    GlobalSliceStore(uint64_t sliceSize) : SliceStore<HashMap>(sliceSize){};
    std::mutex mutex;
};

// A slice store, which contains a set of slices
template<class Key, class Value, uint64_t numberOfSlices = 50>
class PartitionedSliceStore {
    using KeyedSlicePtr = std::unique_ptr<PartitionedKeyedSlice<Key, Value>>;

  public:
    explicit PartitionedSliceStore(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager, uint64_t sliceSize)
        : bufferManager(bufferManager), sliceSize(sliceSize){};
    inline KeyedSlicePtr& findSliceByTs(uint64_t ts) {

        // create the first slice if non is existing [0 - slice size]
        if (empty()) {
            slices[0] = std::make_unique<PartitionedKeyedSlice<uint64_t, uint64_t>>(bufferManager, 0, sliceSize);
            lastIndex++;
        }

        // if the last slice ends before the current event ts we have to add new slices to the end as long es we don't cover ts.
        // if the gap between last and ts is large this could create a large number of slices.
        while (last()->end <= ts) {
            appendNextSlice();
        }

        // find the correct slice
        // calculate the slice offset by the start ts and the event ts.
        auto diffFromStart = ts - first()->start;
        auto sliceOffset = diffFromStart / sliceSize;
        auto sliceIndex = (firstIndex + sliceOffset);
        return slices[getArrayIndex(sliceIndex)];
    }
    inline KeyedSlicePtr& first() { return slices[getArrayIndex(firstIndex)]; }
    inline KeyedSlicePtr& operator[](uint64_t sliceIndex) { return slices[getArrayIndex(sliceIndex)]; }

    inline uint64_t getFirstIndex() { return firstIndex; }

    inline uint64_t getLastIndex() { return lastIndex-1; }

    inline KeyedSlicePtr& last() { return slices[getArrayIndex(getLastIndex())]; }

    inline void eraseFirst(uint64_t n) {
        // std::cout << "erase slice at " << firstIndex << " (" << slices[getArrayIndex(firstIndex)]->start << "-"
        //         << slices[getArrayIndex(firstIndex)]->end << ")" << std::endl;
        this->firstIndex = firstIndex + n;
    }
    std::array<KeyedSlicePtr, numberOfSlices>& getSlices() { return slices; }
    uint64_t lastThreadLocalWatermark = 0;
    uint64_t outputs = 0;

  private:
    uint64_t getArrayIndex(uint64_t sliceIndex) { return sliceIndex % numberOfSlices; }
    bool empty() { return firstIndex == lastIndex; }
    void appendNextSlice() {
        // calculate next slice index
        auto nextSliceIndex = getArrayIndex(lastIndex);
        // currently, we can't handle if we reached the first index
        assert(nextSliceIndex != firstIndex);

        // std::cout << "add slice at " << nextSliceIndex << std::endl;
        if (slices[nextSliceIndex] == nullptr) {
            // create a new keyed slice
            slices[nextSliceIndex] = std::make_unique<PartitionedKeyedSlice<Key, Value>>(bufferManager, last()->end, last()->end + sliceSize);
        } else {
            // re-use old slice
            slices[nextSliceIndex]->reset(last()->end, last()->end + sliceSize);
        }
        this->lastIndex++;
    }
    std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager;
    uint64_t sliceSize;
    std::array<KeyedSlicePtr, numberOfSlices> slices{};
    uint32_t firstIndex = 0;
    uint32_t lastIndex = 0;
};

template<class Key, class Value>
class PartitionedGlobalSlice {
  public:
    PartitionedGlobalSlice(Runtime::BufferManagerPtr bufferManager) : globalState(bufferManager){};
    uint64_t addPartition(std::unique_ptr<Partition<Key, Value>> partition) {
        auto lock = std::unique_lock(addPartitionMutex);
        partitions.emplace_back(std::move(partition));
        return partitions.size();
    }

    const std::vector<std::unique_ptr<Partition<Key, Value>>>& getPartitions() { return partitions; }

    PartitionedHashMap<Key, Value, 1>& getGlobalState() { return globalState; }

  private:
    std::mutex addPartitionMutex;
    // in flight partitions
    std::vector<std::unique_ptr<Partition<Key, Value>>> partitions;
    PartitionedHashMap<Key, Value, 1> globalState;
};

template<class Key, class Value>
class PartitionedGlobalStore {
  public:
    PartitionedGlobalStore(Runtime::BufferManagerPtr bufferManager) : bufferManager(bufferManager){
        triggeredSliceSequenceLog = std::make_shared<SequenceLog<>>();
        windowTriggerProcessor = std::make_shared<LockFreeWatermarkProcessor<>>();
    };
    std::unique_ptr<PartitionedGlobalSlice<Key, Value>>& getSlice(uint64_t sliceIndex) {
        auto lock = std::unique_lock(mutex);
        while (slices.size() <= sliceIndex - startSliceIndex) {
            slices.emplace_back(std::make_unique<PartitionedGlobalSlice<Key, Value>>(bufferManager));
        }
        return slices[sliceIndex - startSliceIndex];
    }

    void removeSlicesTill(uint64_t sliceIndex) {
        auto lock = std::unique_lock(mutex);
        slices.erase(slices.begin() + (sliceIndex - triggeredSlices));
        triggeredSlices = sliceIndex;
    }

    std::shared_ptr<SequenceLog<>> triggeredSliceSequenceLog;
    std::shared_ptr<LockFreeWatermarkProcessor<>> windowTriggerProcessor;
    std::atomic<uint64_t> maxSliceIndex;
    std::atomic<uint64_t> triggeredSlices = 1;

  private:
    std::mutex mutex;
    std::vector<std::unique_ptr<PartitionedGlobalSlice<Key, Value>>> slices;
    Runtime::BufferManagerPtr bufferManager;
    uint64_t startSliceIndex = 0;
};

template<class Key, class Value, uint64_t numberOfPartitions = 2>
class GlobalAggregateStore {
  public:
    GlobalAggregateStore(Runtime::BufferManagerPtr bufferManager) {
        for (uint64_t i = 0; i < numberOfPartitions; i++) {
            partitiones[i] = std::make_unique<PartitionedGlobalStore<Key, Value>>(bufferManager);
        }
    }
    std::unique_ptr<PartitionedGlobalStore<Key, Value>>& getPartition(uint64_t index) { return partitiones[index]; };

  private:
    std::array<std::unique_ptr<PartitionedGlobalStore<Key, Value>>, numberOfPartitions> partitiones;
};

}// namespace NES::Experimental
#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_SLICESTORE_HPP_
