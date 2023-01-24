#include <Execution/DataStructures/ChainedHashMap/ChainedHashMap.hpp>

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
    auto page = reinterpret_cast<Entry*>(this->allocator->allocate(pageSize));
    pages.emplace_back(page);
}

ChainedHashMap::Entry* ChainedHashMap::allocateNewEntry() {
    // check if a new page should be allocated
    if (currentSize % entriesPerPage == 0) {
        auto page = reinterpret_cast<Entry*>(allocator->allocate(pageSize));
        // set entries to zero
        pages.emplace_back(page);
    }
    return entryIndexToAddress(currentSize);
}

ChainedHashMap::Entry* ChainedHashMap::entryIndexToAddress(uint64_t entryIndex) {
    auto pageIndex = entryIndex / entriesPerPage;
    auto* page = pages[pageIndex];
    auto entryOffsetInBuffer = entryIndex - (pageIndex * entriesPerPage);
    return page + entryOffsetInBuffer;
}

ChainedHashMap::~ChainedHashMap() {
    for (auto* page : pages) {
        allocator->deallocate(page, pageSize);
    }
    allocator->deallocate(entries, capacity * sizeof(Entry*));
};

}// namespace NES::Nautilus::Interface