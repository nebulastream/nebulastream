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
//#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/FixedPage.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/FixedPagesLinkedList.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/StreamJoinHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Common.hpp>
#include <zlib.h>

namespace NES::Runtime::Execution::Operators {
StreamJoinHashTable::StreamJoinHashTable(size_t sizeOfRecord,
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

size_t StreamJoinHashTable::getBucketPos(uint64_t hash) const {
    if (mask == 0) {
        return 0;
    }
    return hash % mask;
}

FixedPagesLinkedList* StreamJoinHashTable::getBucketLinkedList(size_t bucketPos) {
    NES_ASSERT2_FMT(bucketPos < buckets.size(), "Tried to access a bucket that does not exist in StreamJoinHashTable!");
    return buckets[bucketPos].get();
}

std::string StreamJoinHashTable::getContentAsString(SchemaPtr schema) const {
    std::stringstream ss;
    //for every bucket
    size_t bucketCnt = 0;
    for (auto& bucket : buckets) {
        ss << "bucket no=" << bucketCnt++;
        //for every page
        size_t pageCnt = 0;
        for (auto& page : bucket->getPages()) {
            ss << " pageNo=" << pageCnt++ << " ";
            ss << page->getContentAsString(schema);
        }
    }
    return ss.str();
}

uint64_t StreamJoinHashTable::getNumberOfTuples() {
    size_t cnt = 0;
    size_t bucketPos = 0;

    for (auto& bucket : buckets) {
        NES_DEBUG("BUCKET {}", bucketPos++);
        for (auto& page : bucket->getPages()) {
            cnt += page->size();
        }
    }
    return cnt;
}

std::string StreamJoinHashTable::getStatistics() {
    size_t cnt = 0;
    std::stringstream ss;
    ss << " numPartitions=" << numPartitions;
    for (auto& bucket : buckets) {
        ss << " BUCKET " << cnt++ << bucket->getStatistics();
    }
    return ss.str();
}

const std::vector<std::unique_ptr<FixedPage>>& StreamJoinHashTable::getPagesForBucket(size_t bucketPos) const {
    return buckets[bucketPos]->getPages();
}

size_t StreamJoinHashTable::getNumItems(size_t bucketPos) const {
    const std::vector<std::unique_ptr<FixedPage>>& pages = getPagesForBucket(bucketPos);
    uint64_t cnt = 0;
    for (auto& elem : pages) {
        cnt += elem->size();
    }
    return cnt;
}

size_t StreamJoinHashTable::getNumPages(size_t bucketPos) const { return buckets[bucketPos]->getPages().size(); }

size_t StreamJoinHashTable::getNumBuckets() const { return buckets.size(); }

}// namespace NES::Runtime::Execution::Operators