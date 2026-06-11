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
#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <SliceStore/Slice.hpp>
#include <Engine.hpp>

namespace NES
{

struct CreateNewHashMapSliceArgs final : CreateNewSlicesArguments
{
    CreateNewHashMapSliceArgs(
        const uint64_t keySize,
        const uint64_t valueSize,
        const uint64_t pageSize,
        const uint64_t numberOfBuckets,
        AbstractBufferProvider* bufferProvider,
        /// Defaults to a disabled (default-constructed) BloomFilterParams so callers that do not use a
        /// BloomFilter (e.g. aggregation) allocate no bloom-bit memory.
        const Nautilus::Interface::BloomFilterParams bloomFilterParams = {})
        : keySize(keySize)
        , valueSize(valueSize)
        , pageSize(pageSize)
        , numberOfBuckets(numberOfBuckets)
        , bufferProvider(bufferProvider)
        , bloomFilterParams(bloomFilterParams)
    {
    }

    ~CreateNewHashMapSliceArgs() override = default;
    uint64_t keySize;
    uint64_t valueSize;
    uint64_t pageSize;
    uint64_t numberOfBuckets;
    AbstractBufferProvider* bufferProvider;
    Nautilus::Interface::BloomFilterParams bloomFilterParams;
};

/// A HashMapSlice stores a number of hashmaps per input stream. We assume that each input stream has the same number of hashmaps
/// We store first all hashmaps of each stream followed by the hashmaps of the next stream, c.f.,
/// +---------------------+---------------------+---------------------+---------------------+---------------------+
/// | Stream 1: [HashMap1][HashMap2][HashMap3]... | Stream 2: [HashMap1][HashMap2][HashMap3]... | ... | Stream N: [HashMap1][HashMap2][HashMap3]... |
/// +---------------------+---------------------+---------------------+---------------------+---------------------+
///
/// As the hashmap might need to clean up its state, we expect multiple clean up functions as part of the @struct CreateNewHashMapSliceArgs
class HashMapSlice : public Slice
{
public:
    explicit HashMapSlice(
        AbstractBufferProvider& bufferProvider,
        SliceStart sliceStart,
        SliceEnd sliceEnd,
        const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
        uint64_t numberOfHashMaps,
        uint64_t numberOfInputStreams);

    /// In our current implementation, we expect one hashmap per worker thread. Thus, we return the number of hashmaps == number of worker threads.
    [[nodiscard]] uint64_t getNumberOfHashMaps() const;
    [[nodiscard]] uint64_t getNumInputStreams() const;
    [[nodiscard]] uint64_t getNumHashMapsPerInputStream() const;

protected:
    /// We use this private struct to store the CreateNewHashMapSliceArgs parameters in a trivially-copyable way, that is also embedded in the header.
    struct HashMapSliceParams
    {
        uint64_t keySize;
        uint64_t valueSize;
        uint64_t pageSize;
        uint64_t numberOfBuckets;

        explicit HashMapSliceParams(const CreateNewHashMapSliceArgs& args)
            : keySize(args.keySize), valueSize(args.valueSize), pageSize(args.pageSize), numberOfBuckets(args.numberOfBuckets)
        {
        }
    };

    static_assert(std::is_trivially_copyable_v<HashMapSliceParams>);

    /// @brief Loads a specific hash map from the slice based on the index
    [[nodiscard]] const TupleBuffer* getHashMapBufferRef(VariableSizedAccess::Index childBufferIndex) const;

    /// metadata
    uint64_t numHashMaps;
    uint64_t numInputStreams;
    uint64_t numHashmapsPerInputStream;
    HashMapSliceParams params;
    /// the hash map buffers for the hash map slice
    std::vector<TupleBuffer> hashMapBuffers;
};

}
