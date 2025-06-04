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
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

struct CreateNewSlicesArguments
{
    virtual ~CreateNewSlicesArguments() = default;
};

struct CreateNewHashMapSliceArgs final : CreateNewSlicesArguments
{
    CreateNewHashMapSliceArgs() = default;
    CreateNewHashMapSliceArgs(uint64_t keySize, uint64_t valueSize, uint64_t pageSize, uint64_t numberOfBuckets)
        : keySize(keySize), valueSize(valueSize), pageSize(pageSize), numberOfBuckets(numberOfBuckets)
    {
    }
    ~CreateNewHashMapSliceArgs() override = default;
    std::optional<uint64_t> keySize;
    std::optional<uint64_t> valueSize;
    std::optional<uint64_t> pageSize;
    std::optional<uint64_t> numberOfBuckets;
};

/// This class represents a single slice for the (keyed) aggregation. It stores the aggregation state in a hashmap.
/// If it is a global aggregation, each hashmap contains a single entry for the keyValue = 0.
/// In our current implementation, we have one hashmap per worker thread.
class HashMapSlice : public Slice
{
public:
    explicit HashMapSlice(SliceStart sliceStart, SliceEnd sliceEnd, uint64_t numberOfHashMaps);

    ~HashMapSlice() override;

    /// Returns the pointer to the underlying hashmap.
    /// IMPORTANT: This method should only be used for passing the hashmap to the nautilus executable.
    [[nodiscard]] virtual Nautilus::Interface::HashMap* getHashMapPtr(WorkerThreadId workerThreadId) const;
    [[nodiscard]] virtual Nautilus::Interface::HashMap*
    getHashMapPtrOrCreate(WorkerThreadId workerThreadId, const CreateNewHashMapSliceArgs& hashMapArgs);


    /// In our current implementation, we expect one hashmap per worker thread. Thus, we return the number of hashmaps == number of worker threads.
    [[nodiscard]] uint64_t getNumberOfHashMaps() const;

    [[nodiscard]] uint64_t getNumberOfTuples() const;
    virtual void
    setCleanupFunction(const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction);

protected:
    std::vector<std::unique_ptr<Nautilus::Interface::HashMap>> hashMaps;
    uint64_t numberOfHashMaps;

private:
    std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)> cleanupFunction;
};

}
