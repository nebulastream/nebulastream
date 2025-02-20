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
#include <Util/Logger/Logger.hpp>
namespace NES::Nautilus::Interface {

/**
 * @brief Function to calculate the capacity of the hash map
 * @param numberOfKeys that are expected to be inserted in the hash map
 * @return
 */
size_t getCapacity(uint64_t numberOfKeys) {
    // this is taken from https://github.com/TimoKersten/db-engine-paradigms/blob/ae3286b279ad26ab294224d630d650bc2f2f3519/include/common/runtime/Hashmap.hpp#L193
    const auto loadFactor = 0.7;
    // __builtin_clzll Returns the number of leading 0-bits in x,
    // starting at the most significant bit position.
    // If x is 0, the result is undefined.
    size_t exp = 64 - __builtin_clzll(numberOfKeys);
    NES_ASSERT(exp < sizeof(size_t) * 8, "invalid exp");
    if (((size_t) 1 << exp) < (numberOfKeys / loadFactor)) {
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
    // clear entry space.
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

void ChainedHashMap::insertPage(int8_t* page, uint64_t numberOfEntries) {
    // insert page
    pages.emplace_back(page);
    // iterate over all entries and inserts them to the hash table.
    for (uint64_t i = 0; i < numberOfEntries; i++) {
        auto entry = reinterpret_cast<Entry*>(page + i * entrySize);
        insert(entry, entry->hash);
    }
}

ChainedHashMap::~ChainedHashMap() {
    for (auto* page : pages) {
        allocator->deallocate(page, pageSize);
    }
    allocator->deallocate(entries, capacity * sizeof(Entry*));
}

int8_t* ChainedHashMap::getPage(uint64_t pageIndex) { return pages[pageIndex]; }

std::vector<uint8_t> ChainedHashMap::serialize() const {
    std::vector<uint8_t> buffer;

    // Serialize metadata
    uint64_t metadata[] = {keySize, valueSize, currentSize, capacity};
    buffer.insert(buffer.end(), reinterpret_cast<uint8_t*>(metadata),
                  reinterpret_cast<uint8_t*>(metadata + 4));

    // Serialize entries
    for (size_t i = 0; i < capacity; i++) {
        Entry* entry = entries[i];
        while (entry) {
            // Store hash
            buffer.insert(buffer.end(), reinterpret_cast<uint8_t*>(&entry->hash),
                          reinterpret_cast<uint8_t*>(&entry->hash) + sizeof(hash_t));

            // Store key and value
            uint8_t* dataPtr = reinterpret_cast<uint8_t*>(entry + 1);
            buffer.insert(buffer.end(), dataPtr, dataPtr + keySize + valueSize);

            entry = entry->next;
        }
    }

    return buffer;
}

// std::vector<uint8_t> ChainedHashMap::serialize() const {
//     // This vector will accumulate our serialized data.
//     std::vector<uint8_t> serialized;
//     // Helper lambda to append raw bytes to our vector.
//     auto append = [&serialized](const void* data, size_t size) {
//         size_t oldSize = serialized.size();
//         serialized.resize(oldSize + size);
//         std::memcpy(serialized.data() + oldSize, data, size);
//     };
//     // --- Write header metadata in a fixed order ---
//     // Append the key size, value size, page size, current size, and capacity.
//     append(&keySize, sizeof(keySize));
//     append(&valueSize, sizeof(valueSize));
//     // We assume that pageSize is 4096 bytes.
//     append(&pageSize, sizeof(pageSize)); // pageSize should be 4096
//     append(&currentSize, sizeof(currentSize));
//     append(&capacity, sizeof(capacity));
//     // Write the number of pages.
//     uint64_t numPages = pages.size();
//     append(&numPages, sizeof(numPages));
//     // Write how many entries fit in a page.
//     append(&entriesPerPage, sizeof(entriesPerPage));
//     // --- Write each page’s state ---
//     // For each page, first write the number of valid entries stored on that page.
//     // (For all pages except possibly the last one, the number of valid entries equals entriesPerPage.)
//     for (size_t i = 0; i < pages.size(); ++i) {
//         uint64_t validEntries = 0;
//         if (i < pages.size() - 1) {
//             validEntries = entriesPerPage;
//         } else {
//             // For the last page, it may be partially filled.
//             validEntries = (currentSize % entriesPerPage == 0) ? entriesPerPage : (currentSize % entriesPerPage);
//         }
//         append(&validEntries, sizeof(validEntries));
//         // Now, append the entire page’s content.
//         // Here we assume that each page is exactly 4096 bytes.
//         append(pages[i], pageSize);
//     }
//     return serialized;
// }


std::unique_ptr<ChainedHashMap> ChainedHashMap::deserialize(const uint8_t* data, size_t length, std::unique_ptr<std::pmr::memory_resource> allocator) {
    // Read metadata
    uint64_t keySize, valueSize, currentSize, capacity;
    std::memcpy(&keySize, data, sizeof(uint64_t));
    std::memcpy(&valueSize, data + sizeof(uint64_t), sizeof(uint64_t));
    std::memcpy(&currentSize, data + 2 * sizeof(uint64_t), sizeof(uint64_t));
    std::memcpy(&capacity, data + 3 * sizeof(uint64_t), sizeof(uint64_t));

    uint64_t estimatedNrEntries = static_cast<uint64_t>(capacity * 0.7);
    while (estimatedNrEntries > 0 && getCapacity(estimatedNrEntries) != capacity) {
        estimatedNrEntries--;
    }

    // Create new hashmap
    auto map = std::make_unique<ChainedHashMap>(keySize, valueSize, estimatedNrEntries, std::move(allocator));

    if (map->capacity != capacity) {
        NES_ERROR("An error occurred while deserializing the ChainedHashMap");
    }

    const uint8_t* ptr = data + 4 * sizeof(uint64_t);
    while (ptr < data + 4 * sizeof(uint64_t) + length) {
        hash_t hash;
        std::memcpy(&hash, ptr, sizeof(hash_t));
        ptr += sizeof(hash_t);

        // Insert new entry
        Entry* newEntry = map->insertEntry(hash);
        std::memcpy(newEntry + 1, ptr, keySize + valueSize);
        ptr += keySize + valueSize;
    }
    if (map->currentSize != currentSize) {
        NES_ERROR("An error occurred while deserializing the ChainedHashMap");
    }

    return map;
}

// std::unique_ptr<ChainedHashMap> ChainedHashMap::deserialize(const std::vector<uint8_t>& serialized, std::unique_ptr<std::pmr::memory_resource> allocator) {
//     size_t offset = 0;
//     auto readField = [&serialized, &offset](void* dest, size_t size) {
//         if (offset + size > serialized.size()) {
//             NES_THROW_RUNTIME_ERROR("Serialized state is truncated.");
//         }
//         std::memcpy(dest, serialized.data() + offset, size);
//         offset += size;
//     };
//     // --- Read header metadata in the same order as written ---
//     uint64_t keySize = 0;
//     uint64_t valueSize = 0;
//     uint64_t pageSize = 0;
//     uint64_t currentSize = 0;
//     uint64_t capacity = 0;
//     uint64_t numPages = 0;
//     uint64_t entriesPerPage = 0;
//     readField(&keySize, sizeof(keySize));
//     readField(&valueSize, sizeof(valueSize));
//     readField(&pageSize, sizeof(pageSize));
//     readField(&currentSize, sizeof(currentSize));
//     readField(&capacity, sizeof(capacity));
//     readField(&numPages, sizeof(numPages));
//     readField(&entriesPerPage, sizeof(entriesPerPage));
//     // Create a new ChainedHashMap.
//     // We pass currentSize as the number of entries (nrEntries). In practice, you might need to adjust this.
//     uint64_t estimatedNrEntries = static_cast<uint64_t>(capacity * 0.7);
//     while (estimatedNrEntries > 0 && getCapacity(estimatedNrEntries) != capacity) {
//         estimatedNrEntries--;
//     }
//     auto newMap = std::make_unique<ChainedHashMap>(
//         keySize, valueSize, estimatedNrEntries, std::move(allocator));
//
//     if (newMap->capacity != capacity) {
//         NES_ERROR("An error occurred while deserializing the ChainedHashMap");
//     }
//
//     // Clear any default pages that the constructor might have allocated.
//     newMap->pages.clear();
//     // --- Read each page’s data ---
//     for (uint64_t i = 0; i < numPages; ++i) {
//         uint64_t validEntries = 0;
//         readField(&validEntries, sizeof(validEntries));
//         // Allocate a new page of size ‘pageSize’ using the map’s allocator.
//         int8_t* page = reinterpret_cast<int8_t*>(newMap->allocator->allocate(pageSize));
//         // Copy the page’s content.
//         readField(page, pageSize);
//         // Append the page pointer to the pages vector.
//         newMap->pages.push_back(page);
//     }
//     // Optionally, verify that we consumed exactly all serialized data.
//     if (offset != serialized.size()) {
//         NES_THROW_RUNTIME_ERROR("Extra data in serialized state.");
//     }
//     // Set the fields that might be used later.
//     newMap->currentSize = currentSize;
//     return newMap;
// }


}// namespace NES::Nautilus::Interface
