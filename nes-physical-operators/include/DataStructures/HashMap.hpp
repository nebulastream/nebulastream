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
#include <iterator>
#include <optional>
#include <span>
#include <vector>
#include <sys/types.h>
#include <val.hpp>
#include "DataTypes/Schema.hpp"
#include "ErrorHandling.hpp"
#include "Nautilus/DataTypes/VarVal.hpp"
#include "Runtime/AbstractBufferProvider.hpp"
#include "Runtime/TupleBuffer.hpp"
#include "Util/Ranges.hpp"

class HashMap
{
public:
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

    struct ConstEntry
    {
        ConstEntry(Entry e) : key(e.key), value(e.value) { }

        std::span<const std::byte> key;
        std::span<const std::byte> value;
    };

    struct Page
    {
        StoragePageHeader* storagePage;

        [[nodiscard]] const StoragePageHeader& header() const { return *storagePage; }

        StoragePageHeader& header() { return *storagePage; }

        std::span<std::byte> entries(const HashMap& map)
        {
            auto entry_size = map.header().key_size + map.header().value_size;
            return std::span(
                reinterpret_cast<std::byte*>(storagePage) + sizeof(StoragePageHeader), map.header().numberOfEntriesPerPage * entry_size);
        }

        ConstEntry at(const HashMap& map, size_t index) const { return const_cast<Page*>(this)->at(map, index); }

        std::optional<Page> nextPage(HashMap& map)
        {
            if (!header().hasNext)
            {
                return {};
            }
            return Page{map.buffer.loadChildBuffer(header().nextPage).getAvailableMemoryArea<StoragePageHeader>().data()};
        }

        Entry at(const HashMap& map, size_t index)
        {
            auto entry_size = map.header().key_size + map.header().value_size;
            auto entry = entries(map).subspan(index * entry_size, entry_size);
            return Entry{entry.subspan(0, map.header().key_size), entry.subspan(map.header().key_size)};
        }

        void insert(std::span<const std::byte> key, std::span<const std::byte> value, HashMap& map)
        {
            auto entry = at(map, header().numEntries++);
            std::ranges::copy(key, entry.key.begin());
            std::ranges::copy(value, entry.value.begin());
        }

        [[nodiscard]] std::optional<const std::span<std::byte>> get(std::span<const std::byte> key, const HashMap& map) const
        {
            return const_cast<Page*>(this)->get(key, map);
        }

        std::optional<std::span<std::byte>> get(std::span<const std::byte> key, const HashMap& map)
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

        StoragePageHeader* prependBuffer(HashMap& map, NES::AbstractBufferProvider& bufferProvider)
        {
            auto pageBuffer = bufferProvider.getBufferBlocking();
            auto storagePage = new (static_cast<void*>(pageBuffer.getAvailableMemoryArea<std::byte>().data())) StoragePageHeader();
            storagePage->hasNext = numberOfEntries != 0;
            storagePage->nextPage = childIndex;
            childIndex = map.buffer.storeChildBuffer(pageBuffer);
            return storagePage;
        }

        Page currentPage(const HashMap& map) const
        {
            return Page{map.buffer.loadChildBuffer(childIndex).getAvailableMemoryArea<StoragePageHeader>().data()};
        }

        Page availablePage(HashMap& map, NES::AbstractBufferProvider& bufferProvider)
        {
            if (numberOfEntries % map.header().numberOfEntriesPerPage == 0)
            {
                return Page{prependBuffer(map, bufferProvider)};
            }
            return currentPage(map);
        }

        bool
        insert(std::span<const std::byte> key, std::span<const std::byte> value, HashMap& map, NES::AbstractBufferProvider& bufferProvider)
        {
            if (auto entry = get(key, map))
            {
                std::ranges::copy(value, entry->begin());
                return false;
            }

            availablePage(map, bufferProvider).insert(key, value, map);
            numberOfEntries++;
            return true;
        }

        [[nodiscard]] std::optional<std::span<const std::byte>> get(std::span<const std::byte> key, const HashMap& map) const
        {
            return const_cast<Bucket*>(this)->get(key, map);
        }

        std::optional<std::span<std::byte>> get(std::span<const std::byte> key, const HashMap& map)
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
                page = Page{map.buffer.loadChildBuffer(page.header().nextPage).getAvailableMemoryArea<StoragePageHeader>().data()};
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

        auto* buckets = reinterpret_cast<Bucket*>(buffer.getAvailableMemoryArea<std::byte>().data() + sizeof(HashMapHeader));
        for (size_t i = 0; i < numberOfBuckets; i++)
        {
            new (buckets) Bucket{};
            buckets++;
        }

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
        auto inserted = buckets()[hash(key) % header().numberOfBuckets].insert(key, value, *this, bufferProvider);

        if (inserted)
        {
            header().numberOfEntries++;
        }

        return inserted;
    }

    std::optional<std::span<const std::byte>>
    get(std::span<const std::byte> key, std::function<size_t(std::span<const std::byte>)> hash) const
    {
        return buckets()[(hash(key) % header().numberOfBuckets)].get(key, *this);
    }

    [[nodiscard]] size_t size() const { return header().numberOfEntries; }

    struct Iterator
    {
        // 1. Required Type Aliases for iterator_traits
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = ConstEntry;
        using pointer = ConstEntry;
        using reference = ConstEntry;

        HashMap* map = nullptr;
        size_t bucketIndex = 0;
        Page page{};
        size_t entryIndex = 0;

        Iterator(HashMap* map, size_t bucket_index, Page page, size_t entry_index)
            : map(map), bucketIndex(bucket_index), page(std::move(page)), entryIndex(entry_index)
        {
        }

        Iterator(HashMap* map) : map(map)
        {
            if (map)
            {
                for (auto [index, bucket] : NES::views::enumerate(map->buckets()))
                {
                    if (bucket.numberOfEntries > 0)
                    {
                        bucketIndex = index;
                        page = bucket.currentPage(*map);
                        entryIndex = 0;
                        return;
                    }
                }
            }
        }

        // 2. Dereference operators
        reference operator*() const { return page.at(*map, entryIndex); }

        pointer operator->() { return page.at(*map, entryIndex); }

        // 3. Prefix increment
        Iterator& operator++()
        {
            if (map == nullptr)
            {
                return *this;
            }

            if (entryIndex + 1 < page.header().numEntries)
            {
                entryIndex++;
                return *this;
            }

            if (auto nextPage = page.nextPage(*map))
            {
                entryIndex = 0;
                page = *nextPage;
                return *this;
            }

            for (auto [index, bucket] : NES::views::enumerate(map->buckets().subspan(bucketIndex + 1)))
            {
                if (bucket.numberOfEntries > 0)
                {
                    bucketIndex += index + 1;
                    page = bucket.currentPage(*map);
                    entryIndex = 0;
                    return *this;
                }
            }

            bucketIndex = 0;
            page = {};
            entryIndex = 0;
            map = nullptr;
            return *this;
        }

        // 4. Postfix increment
        Iterator operator++(int)
        {
            Iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        // 5. Comparison operators
        friend bool operator==(const Iterator& a, const Iterator& b)
        {
            return a.map == b.map && a.bucketIndex == b.bucketIndex && a.entryIndex == b.entryIndex
                && a.page.storagePage == b.page.storagePage;
        };

        friend bool operator!=(const Iterator& a, const Iterator& b)
        {
            return a.map != b.map || a.bucketIndex != b.bucketIndex || a.entryIndex != b.entryIndex
                || a.page.storagePage != b.page.storagePage;
        };
    };

    Iterator begin() { return Iterator{this}; }

    Iterator end() { return Iterator{nullptr}; }
};

struct HashMapRef
{
    nautilus::val<NES::TupleBuffer*> buffer;

    struct Key
    {
        NES::Schema types;
        std::vector<NES::VarVal> keys;
    };

    struct Value
    {
        NES::Schema types;
        std::vector<NES::VarVal> values;
    };

    struct Entry
    {
        std::vector<NES::VarVal> keys;
        std::vector<NES::VarVal> values;
    };

    HashMapRef(const nautilus::val<NES::TupleBuffer*>& buffer, const NES::Schema& key_types, const NES::Schema& value_types)
        : buffer(buffer), keyTypes(key_types), valueTypes(value_types)
    {
    }

    NES::Schema keyTypes;
    NES::Schema valueTypes;

    nautilus::val<bool> insert(const Key& key, const Value& value, nautilus::val<NES::AbstractBufferProvider*> bufferProvider);
    nautilus::val<bool> get(const Key& key, Value& value);

    struct Iterator
    {
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = Entry;
        using pointer = Entry;
        using reference = Entry;

        NES::Schema keyTypes;
        NES::Schema valueTypes;

        nautilus::val<NES::TupleBuffer*> map;
        nautilus::val<uint32_t> bucketIndex;
        nautilus::val<uint32_t> entryIndex;
        nautilus::val<HashMap::StoragePageHeader*> page;

        Iterator(nautilus::val<NES::TupleBuffer*> map, NES::Schema keyTypes, NES::Schema valueTypes);

        // 2. Dereference operators
        reference operator*() const;

        pointer operator->() { return this->operator*(); }

        // 3. Prefix increment
        Iterator& operator++();

        // 4. Postfix increment
        Iterator operator++(int)
        {
            Iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        // 5. Comparison operators
        friend nautilus::val<bool> operator==(const Iterator& a, const Iterator& b)
        {
            return a.map == b.map && a.bucketIndex == b.bucketIndex && a.entryIndex == b.entryIndex && a.page == b.page;
        };

        friend nautilus::val<bool> operator!=(const Iterator& a, const Iterator& b)
        {
            return a.map != b.map || a.bucketIndex != b.bucketIndex || a.entryIndex != b.entryIndex || a.page != b.page;
        };
    };

    Iterator begin() { return Iterator{buffer, keyTypes, valueTypes}; }

    Iterator end() { return Iterator{nullptr, NES::Schema{}, NES::Schema{}}; }
};