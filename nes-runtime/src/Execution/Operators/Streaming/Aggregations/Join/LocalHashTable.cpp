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

#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinBuild.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinUtil.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LocalHashTable.hpp>

namespace NES::Runtime::Execution::Operators {

LocalHashTable::LocalHashTable(size_t sizeOfRecord,
                               size_t numPartitions,
                               std::atomic<uint64_t>& tail,
                               size_t overrunAddress,
                               size_t pageSize) {
    buckets.reserve(numPartitions);
    for (auto i = 0UL; i < numPartitions; ++i) {
        buckets.emplace_back(new FixedPagesLinkedList(tail, overrunAddress, sizeOfRecord, pageSize));
    }
    mask = numPartitions - 1;
}

uint8_t* LocalHashTable::insert(uint64_t key) const {
    auto hashedKey = Util::murmurHash(key);
    return buckets[getBucketPos(hashedKey)]->append(hashedKey);
}

size_t LocalHashTable::getBucketPos(uint64_t key) const {
    if (mask == 0) {
        return 0;
    }

    return key % mask;
}

FixedPagesLinkedList* LocalHashTable::getBucketLinkedList(size_t bucketPos) const {
    NES_ASSERT2_FMT(bucketPos < buckets.size(), "Tried to access a bucket that does not exist in LocalHashTable!");

    return buckets[bucketPos];
}


} // namespace NES::Runtime::Execution::Operators