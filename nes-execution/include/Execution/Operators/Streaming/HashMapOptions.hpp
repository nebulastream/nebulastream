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
#include <ranges>
#include <memory>
#include <utility>
#include <vector>
#include <Execution/Functions/Function.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>


namespace NES::Runtime::Execution::Operators
{

/// Stores members that are needed for both phases of the aggregation, build and probe
class HashMapOptions
{
public:
    friend class HJProbe;
    HashMapOptions(
        std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction,
        std::vector<std::shared_ptr<Functions::Function>> keyFunctions,
        std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldKeys,
        std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldValues,
        const uint64_t entriesPerPage,
        const uint64_t entrySize,
        const uint64_t keySize,
        const uint64_t valueSize,
        const uint64_t pageSize,
        const uint64_t numberOfBuckets)
        : hashFunction(std::move(hashFunction))
        , keyFunctions(std::move(keyFunctions))
        , fieldKeys(std::move(fieldKeys))
        , fieldValues(std::move(fieldValues))
        , entriesPerPage(entriesPerPage)
        , entrySize(entrySize)
        , keySize(keySize)
        , valueSize(valueSize)
        , pageSize(pageSize)
        , numberOfBuckets(numberOfBuckets)
    {
        INVARIANT(entriesPerPage > 0, "The number of entries per page must be greater than 0");
        INVARIANT(entrySize > 0, "The entry size must be greater than 0");
        INVARIANT(valueSize > 0, "The value size must be greater than 0");
        INVARIANT(pageSize > 0, "The page size must be greater than 0");
        INVARIANT(numberOfBuckets > 0, "The number of buckets must be greater than 0");
        INVARIANT(
            entrySize > keySize + valueSize,
            "Entry size {} must be larger than the sum of key {} and value size {}",
            entrySize,
            keySize,
            valueSize);
    }

    HashMapOptions(HashMapOptions&& other) noexcept
        : hashFunction(std::move(other.hashFunction))
        , keyFunctions(std::move(other.keyFunctions))
        , fieldKeys(std::move(other.fieldKeys))
        , fieldValues(std::move(other.fieldValues))
        , entriesPerPage(std::move(other.entriesPerPage))
        , entrySize(std::move(other.entrySize))
        , keySize(std::move(other.keySize))
        , valueSize(std::move(other.valueSize))
        , pageSize(std::move(other.pageSize))
        , numberOfBuckets(std::move(other.numberOfBuckets))
    {
    }

protected:
    /// Method that gets called, once a hash map based slice gets destroyed.
    template <typename NautilusCleanupExecFunc>
    std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>
    getSliceCleanupFunction(std::shared_ptr<NautilusCleanupExecFunc> nautilusCleanupExecutable) const
    {
        return [copyOfCleanupStateNautilusFunction
                = nautilusCleanupExecutable](const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>& hashMaps)
        {
            for (const auto& hashMap :
                 hashMaps | std::views::filter([](const auto& hashMapPtr) { return hashMapPtr and hashMapPtr->getNumberOfTuples() > 0; }))
            {
                /// Calling the compiled nautilus function
                copyOfCleanupStateNautilusFunction->operator()(hashMap.get());
            }
        };
    }

    /// It is fine that these are not nautilus types, because they are only used in the tracing and not in the actual execution
    std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction;
    // todo these keyFunctions can be done in unique_ptr but I would need to implement a clone() in every ExecutableFunction class
    std::vector<std::shared_ptr<Functions::Function>> keyFunctions;
    std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldKeys;
    std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldValues;
    uint64_t entriesPerPage;
    uint64_t entrySize;
    uint64_t keySize;
    uint64_t valueSize;
    uint64_t pageSize;
    uint64_t numberOfBuckets;
};

}
