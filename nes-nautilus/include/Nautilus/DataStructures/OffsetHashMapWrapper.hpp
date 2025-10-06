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
#include <memory>
#include <Nautilus/DataStructures/OffsetBasedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES::DataStructures {

using AbstractBufferProvider = Memory::AbstractBufferProvider;

class OffsetHashMapWrapper : public Nautilus::Interface::HashMap {
private:
    std::unique_ptr<OffsetBasedHashMap> ownedImpl_;
    OffsetBasedHashMap* impl_;

public:
    explicit OffsetHashMapWrapper(OffsetHashMapState& state) 
        : ownedImpl_(std::make_unique<OffsetBasedHashMap>(state)), impl_(ownedImpl_.get()) {}
    
    explicit OffsetHashMapWrapper(std::unique_ptr<OffsetBasedHashMap> impl)
        : ownedImpl_(std::move(impl)), impl_(ownedImpl_.get()) {}
    
    OffsetHashMapWrapper(uint64_t keySize, uint64_t valueSize, uint64_t bucketCount)
        : ownedImpl_(std::make_unique<OffsetBasedHashMap>(keySize, valueSize, bucketCount)), impl_(ownedImpl_.get()) {}
    
    ~OffsetHashMapWrapper() override = default;
    
    Nautilus::Interface::AbstractHashMapEntry* insertEntry(
        Nautilus::Interface::HashFunction::HashValue::raw_type hash, 
        AbstractBufferProvider* bufferProvider) override;
    
    uint64_t getNumberOfTuples() const override {
        return impl_->size();
    }
    
    OffsetBasedHashMap& getImpl() { return *impl_; }
    const OffsetBasedHashMap& getImpl() const { return *impl_; }
    
    OffsetEntry* findBucket(uint64_t hash) {
        return impl_->find(hash);
    }
    
    OffsetEntry* insertOffsetEntry(uint64_t hash, const void* key, const void* value) {
        uint32_t offset = impl_->insert(hash, key, value);
        return getOffsetEntry(offset);
    }
    
    OffsetEntry* getOffsetEntry(uint32_t offset) {
        if (offset == 0) return nullptr;
        auto& state = impl_->extractState();
        return reinterpret_cast<OffsetEntry*>(state.memory.data() + offset);
    }
    
    uint32_t getOffsetFromEntry(OffsetEntry* entry) {
        if (entry == nullptr) return 0;
        auto& state = impl_->extractState();
        return reinterpret_cast<uint8_t*>(entry) - state.memory.data();
    }
    
    // Variable-sized data support
    int8_t* allocateSpaceForVarSized(AbstractBufferProvider* /*bufferProvider*/, size_t neededSize) {
        // Use the underlying OffsetBasedHashMap to allocate variable-sized data
        // For OffsetHashMap, we manage the memory internally rather than using the buffer provider
        uint32_t offset = impl_->allocateVarSized(neededSize);
        return static_cast<int8_t*>(impl_->getVarSizedData(offset));
    }
};

}
