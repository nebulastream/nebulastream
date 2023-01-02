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

#include <Execution/Operators/Streaming/Join/DataStructure/FixedPagesLinkedList.hpp>
#include <Execution/Operators/Streaming/Join/DataStructure/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>

namespace NES::Runtime::Execution::Operators {

LocalHashTable::LocalHashTable(size_t sizeOfRecord,
                               size_t numPartitions,
                               FixedPagesAllocator& fixedPagesAllocator,
                               size_t pageSize)
    : mask(numPartitions - 1) {

    for (auto i = 0UL; i < numPartitions; ++i) {
        buckets.emplace_back(std::make_unique<FixedPagesLinkedList>(fixedPagesAllocator, sizeOfRecord, pageSize));
    }
}

uint8_t* LocalHashTable::insert(uint64_t key) const {
    auto hashedKey = Util::murmurHash(key);
    return buckets[getBucketPos(hashedKey)]->append(hashedKey);
}

size_t LocalHashTable::getBucketPos(uint64_t hash) const {
    if (mask == 0) {
        return 0;
    }
    return hash % mask;
}

FixedPagesLinkedList* LocalHashTable::getBucketLinkedList(size_t bucketPos) {
    NES_ASSERT2_FMT(bucketPos < buckets.size(), "Tried to access a bucket that does not exist in LocalHashTable!");

    return buckets[bucketPos].get();
}

}// namespace NES::Runtime::Execution::Operators