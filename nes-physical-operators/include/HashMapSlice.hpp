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
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <CompilationContext.hpp>

namespace NES
{

/// Carries the config the lowering rule built. Nothing between here and ChainedHashMap::init() may change it.
struct CreateNewHashMapSliceArgs final : CreateNewSlicesArguments
{
    CreateNewHashMapSliceArgs(ChainedHashMapConfig config, AbstractBufferProvider* bufferProvider)
        : config(std::move(config)), bufferProvider(bufferProvider)
    {
    }

    ~CreateNewHashMapSliceArgs() override = default;
    ChainedHashMapConfig config;
    AbstractBufferProvider* bufferProvider;
};

/// Whether a hash map buffer slot has been lazily allocated yet. Backed by uint8_t (not bool) so that
/// std::vector<HashMapBufferState> gives each slot its own byte: unlike std::vector<bool>, which bit-packs
/// multiple slots into a shared word and blocking concurrency
enum class HashMapBufferState : uint8_t
{
    UNINITIALIZED,
    INITIALIZED
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
    /// @brief Returns the hash map buffer at childBufferIndex if it has already been lazily created, or nullptr
    /// otherwise. Never allocates, so it is safe to call without synchronization: it only ever reads state that
    /// is either not-yet-written (nullptr) or was already fully written by whichever thread first-touched this
    /// index via getOrCreateHashMapBufferRef.
    [[nodiscard]] const TupleBuffer* getHashMapBufferRef(ChildBufferIndex childBufferIndex) const;

    /// @brief Loads a specific hash map from the slice based on the index, lazily allocating it on first access.
    [[nodiscard]] const TupleBuffer* getOrCreateHashMapBufferRef(AbstractBufferProvider& bufferProvider, ChildBufferIndex childBufferIndex);

    /// metadata
    uint64_t numHashMaps;
    uint64_t numInputStreams;
    uint64_t numHashmapsPerInputStream;
    ChainedHashMapConfig hashMapConfig;
    /// the hash map buffers for the hash map slice
    std::vector<TupleBuffer> hashMapBuffers;
    /// Holds the state of whether individual tuplebuffers have been allocated.
    std::vector<HashMapBufferState> hashMapBuffersState;
};

}
