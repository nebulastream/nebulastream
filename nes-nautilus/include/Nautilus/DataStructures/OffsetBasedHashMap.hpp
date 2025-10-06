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

#pragma once
#include <vector>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ErrorHandling.hpp>
namespace NES::DataStructures {

struct OffsetEntry {
    uint32_t nextOffset;
    uint32_t padding;    // Explicit padding for 8-byte alignment
    uint64_t hash;
    uint8_t data[];
    
    static constexpr size_t headerSize() {
        return sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t);  // 16 bytes
    }
} __attribute__((packed));

struct OffsetHashMapState {
    std::vector<uint8_t> memory;
    std::vector<uint32_t> chains;
    uint64_t entrySize;
    uint64_t tupleCount;
    
    uint64_t keySize;
    uint64_t valueSize;
    uint64_t bucketCount;
    
    std::vector<uint8_t> varSizedMemory;
    uint64_t varSizedOffset;
};

class OffsetBasedHashMap {
private:
    std::unique_ptr<OffsetHashMapState> ownedState_;
    OffsetHashMapState& state_;

public:
    explicit OffsetBasedHashMap(OffsetHashMapState& state) 
        : ownedState_(nullptr), state_(state) {}
    
    OffsetBasedHashMap(uint64_t keySize, uint64_t valueSize, uint64_t bucketCount)
        : ownedState_(std::make_unique<OffsetHashMapState>(OffsetHashMapState{
            .memory = std::vector<uint8_t>(),
            .chains = std::vector<uint32_t>(bucketCount, 0),
            .entrySize = OffsetEntry::headerSize() + keySize + valueSize,
            .tupleCount = 0,
            .keySize = keySize,
            .valueSize = valueSize,
            .bucketCount = bucketCount,
            .varSizedMemory = std::vector<uint8_t>(),
            .varSizedOffset = 0
          })), state_(*ownedState_) {
        PRECONDITION(OffsetEntry::headerSize() == 16, "OffsetEntry header size must be 16 bytes");
        PRECONDITION(sizeof(OffsetEntry) == 16, "OffsetEntry size must be 16 bytes");
        
        state_.memory.reserve(bucketCount * 64);
    }
    
    OffsetHashMapState& extractState() { return state_; }
    const OffsetHashMapState& extractState() const { return state_; }
    
    static OffsetBasedHashMap fromState(OffsetHashMapState& state) {
        return OffsetBasedHashMap(state);
    }
    
    OffsetEntry* find(uint64_t hash, const void* key) {
        auto chainIdx = hash & (state_.bucketCount - 1);
        auto offset = state_.chains[chainIdx];
        
        while (offset != 0) {
            auto* entry = getEntry(offset);
            if (entry->hash == hash && std::memcmp(entry->data, key, state_.keySize) == 0) {
                return entry;
            }
            offset = entry->nextOffset;
        }
        return nullptr;
    }
    
    OffsetEntry* find(uint64_t hash) {
        auto chainIdx = hash & (state_.bucketCount - 1);
        auto offset = state_.chains[chainIdx];
        
        while (offset != 0) {
            auto* entry = getEntry(offset);
            if (entry->hash == hash) {
                return entry;
            }
            offset = entry->nextOffset;
        }
        return nullptr;
    }
    
    uint32_t insert(uint64_t hash, const void* key, const void* value) {
        uint32_t offset = allocateEntry();
        auto* entry = getEntry(offset);
        
        entry->hash = hash;
        entry->nextOffset = 0;
        
        uint8_t* dataPtr = entry->data;
        std::memcpy(dataPtr, key, state_.keySize);
        std::memcpy(dataPtr + state_.keySize, value, state_.valueSize);
        
        auto chainIdx = hash & (state_.bucketCount - 1);
        entry->nextOffset = state_.chains[chainIdx];
        state_.chains[chainIdx] = offset;
        
        state_.tupleCount++;
        return offset;
    }
    
    bool update(uint64_t hash, const void* key, const void* value) {
        auto* entry = find(hash, key);
        if (entry != nullptr) {
            std::memcpy(entry->data + state_.keySize, value, state_.valueSize);
            return true;
        }
        return false;
    }
    
    uint32_t upsert(uint64_t hash, const void* key, const void* value) {
        auto* entry = find(hash, key);
        if (entry != nullptr) {
            std::memcpy(entry->data + state_.keySize, value, state_.valueSize);
            return reinterpret_cast<uint8_t*>(entry) - state_.memory.data();
        } else {
            return insert(hash, key, value);
        }
    }
    
    uint64_t size() const { return state_.tupleCount; }
    
    void* getKey(OffsetEntry* entry) {
        return entry->data;
    }
    
    void* getValue(OffsetEntry* entry) {
        return entry->data + state_.keySize;
    }
    
    // Variable-sized data allocation
    uint32_t allocateVarSized(size_t size) {
        // Ensure we have enough space
        if (state_.varSizedOffset + size > state_.varSizedMemory.size()) {
            // Grow by at least the requested size plus some buffer
            size_t newSize = state_.varSizedOffset + size + 4096;
            state_.varSizedMemory.resize(newSize);
        }
        
        uint32_t offset = state_.varSizedOffset;
        state_.varSizedOffset += size;
        return offset;
    }
    
    void* getVarSizedData(uint32_t offset) {
        if (offset >= state_.varSizedMemory.size()) {
            return nullptr;
        }
        return state_.varSizedMemory.data() + offset;
    }
    
    class Iterator {
    public:
        Iterator(OffsetBasedHashMap* map, uint32_t bucketIdx, uint32_t offset) 
            : map_(map), bucketIdx_(bucketIdx), currentOffset_(offset) {
            printf("OFFSET_ITER_DEBUG: Iterator created with bucketIdx=%u, offset=%u, totalSize=%lu\n", 
                   bucketIdx, offset, map->size());
            fflush(stdout);
            if (currentOffset_ == 0) {
                findNextEntry();
            }
        }
        
        bool hasNext() const { return currentOffset_ != 0; }
        
        OffsetEntry* next() {
            if (currentOffset_ == 0) {
                printf("OFFSET_ITER_DEBUG: next() called but currentOffset=0, returning nullptr\n");
                fflush(stdout);
                return nullptr;
            }
            
            auto* entry = map_->getEntry(currentOffset_);
            printf("OFFSET_ITER_DEBUG: next() returning entry at offset=%u, hash=%lx, nextOffset=%u\n", 
                   currentOffset_, entry->hash, entry->nextOffset);
            fflush(stdout);
            
            currentOffset_ = entry->nextOffset;
            
            if (currentOffset_ == 0) {
                findNextEntry();
            }
            
            return entry;
        }
        
    private:
        void findNextEntry() {
            printf("OFFSET_ITER_DEBUG: findNextEntry() called, starting bucketIdx=%u\n", bucketIdx_);
            fflush(stdout);
            while (bucketIdx_ < map_->state_.bucketCount && currentOffset_ == 0) {
                currentOffset_ = map_->state_.chains[bucketIdx_];
                printf("OFFSET_ITER_DEBUG: Checking bucket[%u]=%u\n", bucketIdx_, currentOffset_);
                fflush(stdout);
                if (currentOffset_ == 0) {
                    bucketIdx_++;
                }
            }
            printf("OFFSET_ITER_DEBUG: findNextEntry() finished, bucketIdx=%u, currentOffset=%u\n", 
                   bucketIdx_, currentOffset_);
            fflush(stdout);
        }
        
        OffsetBasedHashMap* map_;
        uint32_t bucketIdx_;
        uint32_t currentOffset_;
    };
    
    Iterator begin() {
        return Iterator(this, 0, state_.chains.empty() ? 0 : state_.chains[0]);
    }
    
private:
    OffsetEntry* getEntry(uint32_t offset) {
        return reinterpret_cast<OffsetEntry*>(state_.memory.data() + offset);
    }
    
    uint32_t allocateEntry() {
        uint32_t offset = state_.memory.size();
        printf("OFFSET_ALLOCATE: Current memory.size()=%u, entrySize=%u\n", offset, state_.entrySize);
        if (offset == 0) {
            offset = OffsetEntry::headerSize();
            auto oldSize = state_.memory.size();
            printf("OFFSET_ALLOCATE: First entry resize from %zu to %u\n", oldSize, OffsetEntry::headerSize() + state_.entrySize);
            state_.memory.resize(OffsetEntry::headerSize() + state_.entrySize);  // 16 + 32 = 48, not 64
            printf("OFFSET_ALLOCATE: Before memset: %016lx %016lx %016lx %016lx\n",
                   ((uint64_t*)&state_.memory[oldSize])[0], ((uint64_t*)&state_.memory[oldSize])[1],
                   ((uint64_t*)&state_.memory[oldSize])[2], ((uint64_t*)&state_.memory[oldSize])[3]);
            std::memset(state_.memory.data() + oldSize, 0, state_.memory.size() - oldSize);
            printf("OFFSET_ALLOCATE: After memset: %016lx %016lx %016lx %016lx\n",
                   ((uint64_t*)&state_.memory[oldSize])[0], ((uint64_t*)&state_.memory[oldSize])[1],
                   ((uint64_t*)&state_.memory[oldSize])[2], ((uint64_t*)&state_.memory[oldSize])[3]);
        } else {
            auto oldSize = state_.memory.size();
            printf("OFFSET_ALLOCATE: Additional entry resize from %zu to %u\n", oldSize, offset + state_.entrySize);
            state_.memory.resize(offset + state_.entrySize);
            printf("OFFSET_ALLOCATE: Before memset: %016lx %016lx %016lx %016lx\n",
                   ((uint64_t*)&state_.memory[oldSize])[0], ((uint64_t*)&state_.memory[oldSize])[1],
                   ((uint64_t*)&state_.memory[oldSize])[2], ((uint64_t*)&state_.memory[oldSize])[3]);
            std::memset(state_.memory.data() + oldSize, 0, state_.entrySize);
            printf("OFFSET_ALLOCATE: After memset: %016lx %016lx %016lx %016lx\n",
                   ((uint64_t*)&state_.memory[oldSize])[0], ((uint64_t*)&state_.memory[oldSize])[1],
                   ((uint64_t*)&state_.memory[oldSize])[2], ((uint64_t*)&state_.memory[oldSize])[3]);
        }
        printf("OFFSET_ALLOCATE: Returning offset=%u\n", offset);
        fflush(stdout);
        return offset;
    }
};

}