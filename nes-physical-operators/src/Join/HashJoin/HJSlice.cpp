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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <Join/HashJoin/HJSlice.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>

namespace NES
{
HJSlice::HJSlice(SliceStart sliceStart, SliceEnd sliceEnd, const uint64_t numberOfHashMaps)
    : HashMapSlice(std::move(sliceStart), std::move(sliceEnd), numberOfHashMaps * 2), numberOfHashMapsPerSide(numberOfHashMaps)
{
    rightHashMaps.reserve(numberOfHashMaps);
    leftHashMaps.reserve(numberOfHashMaps);
    for (uint64_t i = 0; i < numberOfHashMapsPerSide; i++)
    {
        rightHashMaps.emplace_back(nullptr);
        leftHashMaps.emplace_back(nullptr);
    }
}

Nautilus::Interface::HashMap* HJSlice::getHashMapPtr(const WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const
{
    switch (buildSide)
    {
        case JoinBuildSideType::Right: {
            const auto pos = workerThreadId % rightHashMaps.size();
            INVARIANT(pos < rightHashMaps.size(), "The worker thread id should be smaller than the number of hashmaps");
            return rightHashMaps[pos].get();
        }
        case JoinBuildSideType::Left: {
            const auto pos = workerThreadId % leftHashMaps.size();
            INVARIANT(pos < leftHashMaps.size(), "The worker thread id should be smaller than the number of hashmaps");
            return leftHashMaps[pos].get();
        }
    }
    std::unreachable();
}

Nautilus::Interface::HashMap* HJSlice::getHashMapPtrOrCreate(
    const WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide, const CreateNewHashMapSliceArgs& hashMapArgs)
{
    switch (buildSide)
    {
        case JoinBuildSideType::Right: {
            const auto pos = workerThreadId % rightHashMaps.size();
            INVARIANT(pos < rightHashMaps.size(), "The worker thread id should be smaller than the number of hashmaps");
            if (rightHashMaps.at(pos) == nullptr)
            {
                /// Hashmap at pos has not been initialized
                PRECONDITION(
                    hashMapArgs.keySize.has_value() and hashMapArgs.valueSize.has_value() and hashMapArgs.numberOfBuckets.has_value()
                        and hashMapArgs.pageSize.has_value(),
                    "Keysize, valueSize, numberOfBuckets, and pageSize MUST BE set before creating a new HashMapSlice!");

                rightHashMaps.at(pos) = std::make_unique<Nautilus::Interface::ChainedHashMap>(
                    hashMapArgs.keySize.value(),
                    hashMapArgs.valueSize.value(),
                    hashMapArgs.numberOfBuckets.value(),
                    hashMapArgs.pageSize.value());
            }
            return rightHashMaps.at(pos).get();
        }
        case JoinBuildSideType::Left: {
            const auto pos = workerThreadId % leftHashMaps.size();
            INVARIANT(pos < leftHashMaps.size(), "The worker thread id should be smaller than the number of hashmaps");
            if (leftHashMaps.at(pos) == nullptr)
            {
                /// Hashmap at pos has not been initialized
                PRECONDITION(
                    hashMapArgs.keySize.has_value() and hashMapArgs.valueSize.has_value() and hashMapArgs.numberOfBuckets.has_value()
                        and hashMapArgs.pageSize.has_value(),
                    "Keysize, valueSize, numberOfBuckets, and pageSize MUST BE set before creating a new HashMapSlice!");

                leftHashMaps.at(pos) = std::make_unique<Nautilus::Interface::ChainedHashMap>(
                    hashMapArgs.keySize.value(),
                    hashMapArgs.valueSize.value(),
                    hashMapArgs.numberOfBuckets.value(),
                    hashMapArgs.pageSize.value());
            }
            return leftHashMaps.at(pos).get();
        }
    }
    std::unreachable();
}

HJSlice::~HJSlice()
{
    /// We need to override the destructor of HashMapSlice, as we have to call two different cleanup functions for the left and right hash maps
    auto cleanupLambda = [](std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>& hashMaps,
                            const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction)
    {
        /// Removing all hashmaps that have no seen tuples. We do not need to call the cleanup function for them
        std::erase_if(
            hashMaps,
            [](const std::unique_ptr<Nautilus::Interface::HashMap>& curHashMap)
            { return curHashMap and curHashMap->getNumberOfTuples() == 0; });

        /// If there exist one hashmap with at least 1 tuple, we expect the cleanup function to be set
        if (not cleanupFunction)
        {
            const bool allNullptr = std::ranges::all_of(hashMaps, [](const auto& ptr) { return ptr == nullptr; });
            INVARIANT(allNullptr, "The cleanup function should be set before the slice is destroyed. Have you called setCleanupFunction?");
            return;
        }

        /// Invoking the cleanup function
        cleanupFunction(hashMaps);
    };

    /// Calling the cleanup function per side
    cleanupLambda(leftHashMaps, cleanupFunctionLeft);
    cleanupLambda(rightHashMaps, cleanupFunctionRight);
    rightHashMaps.clear();
    leftHashMaps.clear();
}

void HJSlice::setCleanupFunction(
    const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction,
    const JoinBuildSideType buildSide)
{
    switch (buildSide)
    {
        case JoinBuildSideType::Right:
            setCleanupFunctionRight(cleanupFunction);
            return;
        case JoinBuildSideType::Left:
            setCleanupFunctionLeft(cleanupFunction);
            return;
            std::unreachable();
    }
}

void HJSlice::setCleanupFunctionLeft(
    const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction)
{
    if (this->cleanupFunctionLeft == nullptr)
    {
        PRECONDITION(
            cleanupFunction != nullptr, "The cleanup function should not be null. Have you passed a null function to setCleanupFunction?");
        this->cleanupFunctionLeft = cleanupFunction;
    }
}

void HJSlice::setCleanupFunctionRight(
    const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction)
{
    if (this->cleanupFunctionRight == nullptr)
    {
        PRECONDITION(
            cleanupFunction != nullptr, "The cleanup function should not be null. Have you passed a null function to setCleanupFunction?");
        this->cleanupFunctionRight = cleanupFunction;
    }
}

uint64_t HJSlice::getNumberOfHashMapsPerSide(const JoinBuildSideType& buildSide) const
{
    switch (buildSide)
    {
        case JoinBuildSideType::Right:
            return rightHashMaps.size();
        case JoinBuildSideType::Left:
            return leftHashMaps.size();
            std::unreachable();
    }
}

void HJSlice::setCleanupFunction(const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>&)
{
    INVARIANT(false, "This method should not be called!");
    std::unreachable();
}


Nautilus::Interface::HashMap* HJSlice::getHashMapPtr(const WorkerThreadId) const
{
    INVARIANT(false, "This method should not be called. Please use the overridden one!");
    std::unreachable();
}

Nautilus::Interface::HashMap* HJSlice::getHashMapPtrOrCreate(const WorkerThreadId, const CreateNewHashMapSliceArgs&)
{
    INVARIANT(false, "This method should not be called. Please use the overridden one!");
    std::unreachable();
}

}
