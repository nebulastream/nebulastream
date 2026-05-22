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
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/SpillBackend.hpp>

namespace NES
{

/// Encodes a single ChainedHashMap into a self-describing byte record and rebuilds it,
/// so a spilled window slice can round-trip through a SpillBackend (Phase 2).
///
/// Format: the chain `next` pointers are absolute addresses and are dropped on the way
/// out; the chain is a derived index that is rebuilt on the way in by replaying
/// `ChainedHashMap::insertEntry` for each entry. Only the portable per-entry payload
/// (hash + fixed-size key/value bytes) is persisted.
///
/// PRECONDITION: the map uses fixed-size keys and values. Variable-sized keys/values
/// (the map's varSizedSpace) store out-of-line pointers that do not survive a disk
/// round-trip and are not supported in this phase.
///
/// `entrySize`, `pageSize`, and `numberOfBuckets` describe the map's layout; the caller
/// supplies them (in the integration they come from the same CreateNewHashMapSliceArgs used
/// to build the map) so this serializer does not reach into ChainedHashMap's private state.
/// `numberOfBuckets` is the pre-rounding value passed to the ChainedHashMap constructor — NOT
/// the post-rounding chain count — so reconstruction reproduces the exact same chain count and
/// repeated spill/unspill cycles are idempotent.
class SliceSerializer
{
public:
    /// Magic marker at the start of every record, for corruption detection (big-endian ASCII 'N','S','S','1').
    static constexpr uint32_t MAGIC = 0x4E535331;
    /// On-disk format version; bumped on any layout change.
    static constexpr uint32_t VERSION = 1;
    /// Size of the fixed record header in bytes (magic, version, entrySize, pageSize, numberOfBuckets, numEntries).
    static constexpr std::size_t HEADER_SIZE = (sizeof(uint32_t) * 2) + (sizeof(uint64_t) * 4);

    /// Encode `map` into a record. `entrySize`/`pageSize`/`numberOfBuckets` must match the
    /// values the map was constructed with. Throws CannotSerialize if `entrySize` is smaller
    /// than an entry header.
    static SpillRecord serialize(const ChainedHashMap& map, uint64_t entrySize, uint64_t pageSize, uint64_t numberOfBuckets);

    /// Decode a record produced by serialize() into a fresh ChainedHashMap, renting pages
    /// from `bufferProvider`. Throws CannotDeserialize on a malformed or truncated record.
    static std::unique_ptr<ChainedHashMap> deserialize(std::span<const std::byte> record, AbstractBufferProvider& bufferProvider);
};

}
