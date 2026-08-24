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
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <ErrorHandling.hpp>

namespace NES
{

class BinaryStore
{
public:
    using Blob = std::vector<std::byte>;

private:
    std::unordered_map<uint64_t, Blob> blobs;
    uint64_t nextId = 0;

public:
    BinaryStore() = default;

    /// Ids are a per-store counter, so storing identical bytes twice yields two entries. The store
    /// deliberately does not deduplicate: the id scheme is private to this class, and content
    /// addressing can be reintroduced later without any caller noticing.
    uint64_t store(const std::span<const std::byte> blob)
    {
        const auto id = nextId++;
        blobs.emplace(id, Blob{blob.begin(), blob.end()});
        return id;
    }

    void insert(const uint64_t id, Blob blob) { blobs.insert_or_assign(id, std::move(blob)); }

    [[nodiscard]] std::span<const std::byte> read(const uint64_t id) const
    {
        const auto it = blobs.find(id);
        if (it == blobs.end())
        {
            throw CannotDeserialize("Binary store has no blob for id {}", id);
        }
        return it->second;
    }

    [[nodiscard]] bool contains(const uint64_t id) const { return blobs.contains(id); }
    [[nodiscard]] size_t size() const { return blobs.size(); }

    [[nodiscard]] const std::unordered_map<uint64_t, Blob>& entries() const { return blobs; }
};

struct BlobWriter
{
    std::shared_ptr<BinaryStore> store;

    [[nodiscard]] uint64_t write(const std::span<const std::byte> blob) const
    {
        INVARIANT(store != nullptr, "BlobWriter used without a store");
        return store->store(blob);
    }
};

struct BlobReader
{
    std::shared_ptr<const BinaryStore> store;

    [[nodiscard]] std::span<const std::byte> read(const uint64_t id) const
    {
        INVARIANT(store != nullptr, "BlobReader used without a store");
        return store->read(id);
    }
};

}
