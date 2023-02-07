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

#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>

namespace NES::Nautilus::Interface {

size_t getCapacity(uint64_t nrEntries) {
    const auto loadFactor = 0.7;
    size_t exp = 64 - __builtin_clzll(nrEntries);
    NES_ASSERT(exp < sizeof(size_t) * 8, "invalid exp");
    if (((size_t) 1 << exp) < nrEntries / loadFactor) {
        exp++;
    }
    return ((size_t) 1) << exp;
}

int8_t* ChainedHashMap::getPage(uint64_t pageIndex) {
    return pages[pageIndex];
}

ChainedHashMap::ChainedHashMap(uint64_t keySize,
                               uint64_t valueSize,
                               uint64_t nrEntries,
                               std::unique_ptr<std::pmr::memory_resource> allocator,
                               size_t pageSize)
    : allocator(std::move(allocator)), pageSize(pageSize), keySize(keySize), valueSize(valueSize),
      /*calculate entry size*/ entrySize(sizeof(Entry) + keySize + valueSize),
      /*calculate number of entries per page*/ entriesPerPage(pageSize / entrySize),
      /*calculate number of buckets*/ capacity(getCapacity(nrEntries)), mask(capacity - 1), currentSize(0) {
    NES_ASSERT(nrEntries != 0, "invalid num entries");
    NES_ASSERT(keySize != 0, "invalid key size");
    NES_ASSERT(pageSize >= entrySize, "invalid page size");

    // allocate entries
    entries = reinterpret_cast<Entry**>(this->allocator->allocate(capacity * sizeof(Entry*)));
    std::memset(entries, 0, capacity * sizeof(Entry*));

    // allocate initial page
    auto page = reinterpret_cast<int8_t*>(this->allocator->allocate(pageSize));
    pages.emplace_back(page);
}


ChainedHashMap::Entry* ChainedHashMap::allocateNewEntry() {
    // check if a new page should be allocated
    if (currentSize % entriesPerPage == 0) {
        auto page = reinterpret_cast<int8_t*>(allocator->allocate(pageSize));
        pages.emplace_back(page);
    }
    return entryIndexToAddress(currentSize);
}

ChainedHashMap::Entry* ChainedHashMap::entryIndexToAddress(uint64_t entryIndex) {
    auto pageIndex = entryIndex / entriesPerPage;
    int8_t* page = pages[pageIndex];
    auto entryOffsetInBuffer = entryIndex - (pageIndex * entriesPerPage);
    return reinterpret_cast<Entry*>(page + (entryOffsetInBuffer * entrySize));
}

uint64_t ChainedHashMap::getCurrentSize() const { return currentSize; }

ChainedHashMap::~ChainedHashMap() {
    for (auto* page : pages) {
        allocator->deallocate(page, pageSize);
    }
    allocator->deallocate(entries, capacity * sizeof(Entry*));
}

}// namespace NES::Nautilus::Interface