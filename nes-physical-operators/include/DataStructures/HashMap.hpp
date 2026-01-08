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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <sys/types.h>

#include "ErrorHandling.hpp"
#include "Runtime/AbstractBufferProvider.hpp"
#include "Runtime/TupleBuffer.hpp"

class HashMap
{
    struct StoragePageHeader
    {
        uint32_t magic = 0xABADBABE;
        uint32_t numEntries = 0;
        NES::VariableSizedAccess::Index nextPage = NES::VariableSizedAccess::Index(0);
        bool hasNext = false;
    };

    struct Entry
    {
        std::span<std::byte> key;
        std::span<std::byte> value;
    };

    struct Page
    {
        NES::TupleBuffer storagePage;

        StoragePageHeader& header() { return storagePage.getAvailableMemoryArea<StoragePageHeader>()[0]; }

        Entry at(const HashMap& map, size_t index)
        {
            auto entry_size = map.header().key_size + map.header().value_size;
            auto entries = storagePage.getAvailableMemoryArea<std::byte>().subspan(sizeof(StoragePageHeader));
            auto entry = entries.subspan(index * entry_size, entry_size);
            return Entry{entry.subspan(0, map.header().key_size), entry.subspan(map.header().key_size)};
        }

        void insert(std::span<const std::byte> key, std::span<const std::byte> value, HashMap& map)
        {
            auto entry = at(map, header().numEntries++);
            std::ranges::copy(key, entry.key.begin());
            std::ranges::copy(value, entry.value.begin());
        }

        std::optional<std::span<const std::byte>> get(std::span<const std::byte> key, const HashMap& map)
        {
            for (ssize_t i = header().numEntries - 1; i >= 0; i--)
            {
                auto entry = at(map, i);
                if (std::ranges::equal(entry.key, key))
                {
                    return entry.value;
                }
            }
            return std::nullopt;
        }
    };

    struct Bucket
    {
        uint32_t numberOfEntries = 0;
        NES::VariableSizedAccess::Index childIndex = NES::VariableSizedAccess::Index(0);

        NES::TupleBuffer prependBuffer(HashMap& map, NES::AbstractBufferProvider& bufferProvider)
        {
            auto pageBuffer = bufferProvider.getBufferBlocking();
            auto storagePage = new (static_cast<void*>(pageBuffer.getAvailableMemoryArea<std::byte>().data())) StoragePageHeader();
            storagePage->hasNext = numberOfEntries != 0;
            storagePage->nextPage = childIndex;
            childIndex = map.buffer.storeChildBuffer(pageBuffer);
            return map.buffer.loadChildBuffer(childIndex);
        }

        Page currentPage(const HashMap& map) const { return Page{map.buffer.loadChildBuffer(childIndex)}; }

        Page availablePage(HashMap& map, NES::AbstractBufferProvider& bufferProvider)
        {
            if (numberOfEntries % map.header().numberOfEntriesPerPage == 0)
            {
                return Page{prependBuffer(map, bufferProvider)};
            }
            return currentPage(map);
        }

        void
        insert(std::span<const std::byte> key, std::span<const std::byte> value, HashMap& map, NES::AbstractBufferProvider& bufferProvider)
        {
            availablePage(map, bufferProvider).insert(key, value, map);
            numberOfEntries++;
        }

        std::optional<std::span<const std::byte>> get(std::span<const std::byte> key, const HashMap& map) const
        {
            if (numberOfEntries == 0)
            {
                return std::nullopt;
            }

            auto page = currentPage(map);

            while (true)
            {
                if (auto result = page.get(key, map))
                {
                    return result;
                }
                if (!page.header().hasNext)
                {
                    return std::nullopt;
                }
                page = Page{map.buffer.loadChildBuffer(page.header().nextPage)};
            }
        }
    };

    struct HashMapHeader
    {
        uint64_t magic = 0xDEADBEEF;
        uint64_t numberOfEntries = 0;
        uint32_t numberOfBuckets;
        uint32_t key_size;
        uint32_t value_size;
        uint32_t numberOfEntriesPerPage;

        HashMapHeader(uint32_t numberOfBuckets, uint32_t key_size, uint32_t value_size, uint32_t numberOfEntriesPerPage)
            : numberOfBuckets(numberOfBuckets), key_size(key_size), value_size(value_size), numberOfEntriesPerPage(numberOfEntriesPerPage)
        {
        }
    };

    NES::TupleBuffer buffer;
    explicit HashMap(NES::TupleBuffer buffer) : buffer(std::move(buffer)) { };

    HashMapHeader& header() { return buffer.getAvailableMemoryArea<HashMapHeader>()[0]; }

    const HashMapHeader& header() const { return buffer.getAvailableMemoryArea<HashMapHeader>()[0]; }

    [[nodiscard]] std::span<const Bucket> buckets() const
    {
        auto* buckets = reinterpret_cast<const Bucket*>(buffer.getAvailableMemoryArea<std::byte>().data() + sizeof(HashMapHeader));
        return {buckets, header().numberOfBuckets};
    }

    std::span<Bucket> buckets()
    {
        auto* buckets = reinterpret_cast<Bucket*>(buffer.getAvailableMemoryArea<std::byte>().data() + sizeof(HashMapHeader));
        return {buckets, header().numberOfBuckets};
    }

public:
    struct Options
    {
        uint32_t keySize;
        uint32_t valueSize;
    };

    static NES::TupleBuffer createHashMap(NES::AbstractBufferProvider& bufferProvider, const Options& options)
    {
        auto buffer = bufferProvider.getBufferBlocking();
        const uint32_t numberOfBuckets = (buffer.getBufferSize() - sizeof(HashMapHeader)) / sizeof(Bucket);
        const uint32_t numberOfEntriesPerPage
            = (buffer.getBufferSize() - sizeof(StoragePageHeader)) / (options.keySize + options.valueSize);
        new (static_cast<void*>(buffer.getAvailableMemoryArea<std::byte>().data()))
            HashMapHeader{numberOfBuckets, options.keySize, options.valueSize, numberOfEntriesPerPage};
        return buffer;
    }

    static HashMap load(NES::TupleBuffer buffer)
    {
        PRECONDITION(buffer.getAvailableMemoryArea<HashMapHeader>()[0].magic = 0xDEADBEEF, "Attempting to load a not initialized hashmap");
        return HashMap{std::move(buffer)};
    }

    auto insert(
        std::span<const std::byte> key,
        std::span<const std::byte> value,
        NES::AbstractBufferProvider& bufferProvider,
        std::function<size_t(std::span<const std::byte>)> hash)
    {
        header().numberOfEntries++;
        return buckets()[hash(key) % header().numberOfBuckets].insert(key, value, *this, bufferProvider);
    }

    std::optional<std::span<const std::byte>>
    get(std::span<const std::byte> key, std::function<size_t(std::span<const std::byte>)> hash) const
    {
        return buckets()[(hash(key) % header().numberOfBuckets)].get(key, *this);
    }

    [[nodiscard]] size_t size() const { return header().numberOfEntries; }
};