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
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <Engine.hpp>

namespace NES
{

struct CreateNewHashMapSliceArgs final : CreateNewSlicesArguments
{
    using NautilusCleanupExec = nautilus::engine::CallableFunction<void, Nautilus::Interface::HashMap*>;

    CreateNewHashMapSliceArgs(
        std::vector<std::shared_ptr<NautilusCleanupExec>> nautilusCleanup,
        const uint64_t keySize,
        const uint64_t valueSize,
        const uint64_t pageSize,
        const uint64_t numberOfBuckets)
        : nautilusCleanup(std::move(nautilusCleanup))
        , keySize(keySize)
        , valueSize(valueSize)
        , pageSize(pageSize)
        , numberOfBuckets(numberOfBuckets)
    {
    }

    ~CreateNewHashMapSliceArgs() override = default;
    std::vector<std::shared_ptr<NautilusCleanupExec>> nautilusCleanup;
    uint64_t keySize;
    uint64_t valueSize;
    uint64_t pageSize;
    uint64_t numberOfBuckets;
};

/// A HashMapSlice stores a number of hashmaps per input stream. We assume that each input stream has the same number of hashmaps
/// We store first all hashmaps of each stream followed by the hashmaps of the next stream, c.f.,
/// +---------------------+---------------------+---------------------+---------------------+---------------------+
/// | Stream 1: [HashMap1][HashMap2][HashMap3]... | Stream 2: [HashMap1][HashMap2][HashMap3]... | ... | Stream N: [HashMap1][HashMap2][HashMap3]... |
/// +---------------------+---------------------+---------------------+---------------------+---------------------+
///
/// As the hashmap might need to clean up its state, we expect multiple clean up functions as part of the @struct CreateNewHashMapSliceArgs
/// For each stream, we expect one cleanup function and once this HashMapSlice gets destroyed they are being called.
class HashMapSlice : public Slice
{
public:
    explicit HashMapSlice(
        SliceStart sliceStart,
        SliceEnd sliceEnd,
        const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
        uint64_t numberOfHashMaps,
        uint64_t numberOfInputStreams);

    ~HashMapSlice() override;

    /// In our current implementation, we expect one hashmap per worker thread. Thus, we return the number of hashmaps == number of worker threads.
    [[nodiscard]] uint64_t getNumberOfHashMaps() const;

    [[nodiscard]] uint64_t getNumberOfTuples() const;

protected:
    std::vector<std::unique_ptr<Nautilus::Interface::HashMap>> hashMaps;
    CreateNewHashMapSliceArgs createNewHashMapSliceArgs;
    uint64_t numberOfHashMapsPerInputStream;
    uint64_t numberOfInputStreams;
};

}
