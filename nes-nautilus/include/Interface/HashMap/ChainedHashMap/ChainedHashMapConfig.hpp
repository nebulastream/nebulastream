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

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/Hash/HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/FieldOffsets.hpp>

namespace NES
{

/// Everything about a ChainedHashMap that the query compiler decides: its sizing, its optional BloomFilter,
/// the key/value layout of an entry, and the hash function. This is the *only* hash map options type — it is
/// fixed at query-compilation time, travels to the physical operators as a plain C++ value, and is read
/// during tracing only. Nothing may re-derive any of it elsewhere.
///
/// It is deliberately not stored in the map's buffer, so a running query cannot change the sizing underneath
/// a compiled pipeline. Runtime code inside a non-capturing nautilus::invoke lambda cannot receive this
/// struct, so the entry points such code needs (ChainedHashMap::init, insertEntry, calculateBufferSize) also
/// exist in a scalar form taking the handful of numbers involved. Those numbers must always come from here.
struct ChainedHashMapConfig
{
    /// Fraction of the chains array the expected key count is allowed to fill before numberOfChains() rounds
    /// up to the next power of two. Part of the config because it is what turns numberOfBuckets into the
    /// map's actual chain count.
    static constexpr auto assumedLoadFactor = 0.75;

    uint64_t entrySize = 0;
    uint64_t numberOfBuckets = 0;
    uint64_t pageSize = 0;
    /// Empty when this map runs without an in-map BloomFilter.
    std::optional<Nautilus::Interface::BloomFilterParams> bloomFilterParams;
    std::vector<FieldOffsets> fieldKeys;
    std::vector<FieldOffsets> fieldValues;
    std::shared_ptr<const HashFunction> hashFunction;

    /// Turning numberOfBuckets into a chain count, a mask or a buffer size needs the map's memory layout, so
    /// those live on ChainedHashMap itself: calculateNumberOfChains() and calculateBufferSize(). Only the
    /// derivations that need nothing but this struct stay here.
    [[nodiscard]] uint64_t entriesPerPage() const { return pageSize / entrySize; }

    /// Bytes reserved inline for the BloomFilter bit area. 0 when the filter is disabled.
    [[nodiscard]] uint64_t bloomFilterMemAreaSize() const { return bloomFilterParams ? bloomFilterParams->allocationByteCount() : 0; }
};

}
