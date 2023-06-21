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

#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/FixedPagesLinkedList.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/HashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Common.hpp>
#include <zlib.h>

namespace NES::Runtime::Execution::Operators {
HashTable::HashTable(size_t sizeOfRecord,
                     size_t numPartitions,
                     FixedPagesAllocator& fixedPagesAllocator,
                     size_t pageSize,
                     size_t preAllocPageSizeCnt)
    : mask(numPartitions - 1), numPartitions(numPartitions) {

    for (auto i = 0UL; i < numPartitions; ++i) {
        buckets.emplace_back(
            std::make_unique<FixedPagesLinkedList>(fixedPagesAllocator, sizeOfRecord, pageSize, preAllocPageSizeCnt));
    }
}

size_t HashTable::getBucketPos(uint64_t hash) const {
    if (mask == 0) {
        return 0;
    }
    return hash % mask;
}

FixedPagesLinkedList* HashTable::getBucketLinkedList(size_t bucketPos) {
    NES_ASSERT2_FMT(bucketPos < buckets.size(), "Tried to access a bucket that does not exist in HashTable!");
    return buckets[bucketPos].get();
}

uint64_t HashTable::getNumberOfTuples() {
    size_t cnt = 0;
    for (auto& bucket : buckets) {
        NES_TRACE2("BUCKET ", cnt++);
        for (auto& page : bucket->getPages()) {
            cnt += page->size();
        }
    }
    return cnt;
}

std::string HashTable::getStatistics() {
    size_t cnt = 0;
    std::stringstream ss;
    ss << " numPartitions=" << numPartitions;
    for (auto& bucket : buckets) {
        ss << " BUCKET " << cnt++ << bucket->getStatistics();
    }
    return ss.str();
}

}// namespace NES::Runtime::Execution::Operators