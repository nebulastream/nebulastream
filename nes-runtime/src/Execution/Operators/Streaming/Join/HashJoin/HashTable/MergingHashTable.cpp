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

#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/MergingHashTable.hpp>
#include <Util/Logger/Logger.hpp>
#include <atomic>

namespace NES::Runtime::Execution::Operators {
void MergingHashTable::insertBucket(size_t bucketPos, const FixedPagesLinkedList* pagesLinkedList) {
    //Note that this function creates a new Fixpage (that only moves ptr) instead of reusing the existing one
    auto& numItems = bucketNumItems[bucketPos];
    auto& numPages = bucketNumPages[bucketPos];
    auto lockedBucketHeads = bucketHeads[bucketPos].wlock();

    for (auto&& page : pagesLinkedList->getPages()) {
        lockedBucketHeads->emplace_back(page);
    }

    numPages.fetch_add(pagesLinkedList->getPages().size(), std::memory_order::seq_cst);
}

Nautilus::Interface::FixedPage* MergingHashTable::getPageFromBucketAtPos(size_t bucketPos, size_t pageNo) {
    auto lockedBucketHeads = bucketHeads[bucketPos].rlock();
    NES_ASSERT2_FMT(lockedBucketHeads->size() > pageNo, "Accessing a page that does not exist!");
    return lockedBucketHeads->at(pageNo).get();
}

uint64_t MergingHashTable::getNumberOfTuplesForPage(size_t bucketPos, size_t pageNo) {
    auto lockedBucketHeads = bucketHeads[bucketPos].rlock();
    NES_ASSERT2_FMT(lockedBucketHeads->size() > pageNo, "Accessing a page that does not exist!");
    return lockedBucketHeads->at(pageNo)->size();
}

uint8_t* getNumberOfTuplesForPage(size_t bucket, size_t page);

size_t MergingHashTable::getNumBuckets() const { return bucketNumPages.size(); }

size_t MergingHashTable::getNumItems(size_t bucketPos) const { return bucketNumItems[bucketPos].load(); }

size_t MergingHashTable::getNumPages(size_t bucketPos) const { return bucketNumPages[bucketPos].load(); }

MergingHashTable::MergingHashTable(size_t numBuckets)
    : bucketHeads(numBuckets), bucketNumItems(numBuckets), bucketNumPages(numBuckets) {}

std::string MergingHashTable::getContentAsString(SchemaPtr schema) const {
    std::stringstream ss;
    //for every bucket
    size_t bucketCnt = 0;
    for (auto& bucket : bucketHeads) {
        ss << "bucket no=" << bucketCnt++;
        //for every page
        auto pageCnt = 0_u64;
        auto lockedBucketHeads = bucket.rlock();
        for (auto& page : *lockedBucketHeads) {
            ss << " pageNo=" << pageCnt++ << " ";
            ss << page->getContentAsString(schema);
        }
    }
    return ss.str();
}
}// namespace NES::Runtime::Execution::Operators
