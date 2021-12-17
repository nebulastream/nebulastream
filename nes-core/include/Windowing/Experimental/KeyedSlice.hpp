//
// Created by pgrulich on 13.12.21.
//

#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDSLICE_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDSLICE_HPP_

#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Windowing/Experimental/robin_hood.h>
#include <utility>

namespace NES::Experimental {

// A single slice, which contains all keys
template<class HashMap>
class KeyedSlice {
  public:
    KeyedSlice(uint64_t start, uint64_t end) : start(start), end(end){};
    void reset(uint64_t start, uint64_t end) {
        this->start = start;
        this->end = end;
        map.clear();
    }
    uint64_t start;
    uint64_t end;
    HashMap map;
};

template<class KeyType, class ValueType>
struct Entry {
    KeyType key;
    uint64_t hash;
    ValueType value;
};

template<class KeyType, class ValueType>
class Partition {
  public:
    Partition(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager)
        : bufferManager(bufferManager), entrySize(sizeof(Entry<KeyType, ValueType>)),
          entriesPerBuffer(bufferManager->getBufferSize() / entrySize) {}

    uint8_t* entryIndexToAddress(uint64_t entry) {
        auto bufferIndex = entry / entriesPerBuffer;
        auto& buffer = tupleBuffers[bufferIndex];
        auto entryOffsetInBuffer = entry - (bufferIndex * entriesPerBuffer);
        return buffer.getBuffer() + (entryOffsetInBuffer * entrySize);
    }

    uint8_t* allocateNewEntry() {
        if (maxEntry % entriesPerBuffer == 0) {
            Runtime::TupleBuffer buffer = bufferManager->getBufferBlocking();
            tupleBuffers.emplace_back(std::move(buffer));
        }
        return entryIndexToAddress(maxEntry++);
    }

    Entry<KeyType, ValueType>* operator[](uint64_t index) { return (Entry<KeyType, ValueType>*) entryIndexToAddress(index); }

    uint64_t size() { return maxEntry; }

  private:
    std::vector<Runtime::TupleBuffer> tupleBuffers;
    std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager;
    uint64_t entrySize;
    uint64_t entriesPerBuffer;
    uint64_t maxEntry = 0;
};

template<class KeyType, class ValueType, uint64_t numberOfPartitions = 2>
class PartitionedHashMap {
  public:
    PartitionedHashMap(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager)
        : bufferManager(bufferManager), entrySize(sizeof(Entry<KeyType, ValueType>)),
          entriesPerBuffer(bufferManager->getBufferSize() / sizeof(Entry<KeyType, ValueType>)) {
        clear();
    }

    Entry<KeyType, ValueType>* getEntry(KeyType key) {
        const auto hash = getHashEntry(key);
        auto value = map.find(key);
        if (value == map.end()) {
            auto partitionIndex = hash % numberOfPartitions;
            auto nextAddress = partitions[partitionIndex]->allocateNewEntry();
            map[key] = nextAddress;
            auto* entry = ((Entry<KeyType, ValueType>*) nextAddress);
            entry->hash = hash;
            entry->key = key;
            entry->value = 0;
            return entry;
        }
        return ((Entry<KeyType, ValueType>*) value->second);
    }

    void clear() {
        map.clear();
        for (uint64_t i = 0; i < numberOfPartitions; i++) {
            partitions[i] = std::move(std::make_unique<Partition<KeyType, ValueType>>(bufferManager));
        }
    }

    uint64_t getNumberOfPartitions() { return numberOfPartitions; }
    std::array<std::unique_ptr<Partition<KeyType, ValueType>>, numberOfPartitions>& getPartitions() { return partitions; }

    uint64_t getHashEntry(KeyType key) { return hash(key); }

     uint64_t getSize() const {
        uint64_t size = 0;
        for (uint64_t p = 0; p < numberOfPartitions; p++) {
            size = size + partitions[p]->size();
        }
        return size;
    }

    std::unique_ptr<Partition<KeyType, ValueType>>& getPartition(uint64_t index) { return partitions[index]; }

  private:
    std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager;
    robin_hood::hash<KeyType> hash;
    robin_hood::unordered_map<KeyType, uint8_t*> map;
    std::array<std::unique_ptr<Partition<KeyType, ValueType>>, numberOfPartitions> partitions;
    uint64_t entrySize;
    uint64_t entriesPerBuffer;
};

template<class KeyType, class ValueType, uint64_t numberOfPartitions = 2>
class PartitionedKeyedSlice {
  public:
    PartitionedKeyedSlice(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager, uint64_t start, uint64_t end)
        : start(start), end(end), sliceIndex(start/(end-start)), map(bufferManager){};
    void reset(uint64_t start, uint64_t end) {
        this->start = start;
        this->end = end;
        this->sliceIndex = start/(end-start);
        map.clear();
    }
    uint64_t start;
    uint64_t end;
    uint64_t sliceIndex;
    PartitionedHashMap<KeyType, ValueType> map;
};

}// namespace NES::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDSLICE_HPP_
