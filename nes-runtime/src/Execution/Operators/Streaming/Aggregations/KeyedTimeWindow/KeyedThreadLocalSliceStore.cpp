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

#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>

namespace NES::Runtime::Execution::Operators {

KeyedThreadLocalSliceStore::KeyedThreadLocalSliceStore(uint64_t keySize,
                                                       uint64_t valueSize,
                                                       uint64_t windowSize,
                                                       uint64_t windowSlide,
                                                       uint64_t numberOfKeys)
    : ThreadLocalSliceStore(windowSize, windowSlide), keySize(keySize), valueSize(valueSize), numberOfKeys(numberOfKeys) {}

KeyedSlicePtr KeyedThreadLocalSliceStore::allocateNewSlice(uint64_t startTs, uint64_t endTs) {
    // allocate hash map
    NES_DEBUG("allocateNewSlice {}-{}", startTs, endTs);
    auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
    auto hashMap = std::make_unique<Nautilus::Interface::ChainedHashMap>(keySize, valueSize, numberOfKeys, std::move(allocator));
    return std::make_unique<KeyedSlice>(std::move(hashMap), startTs, endTs);
}

}// namespace NES::Runtime::Execution::Operators