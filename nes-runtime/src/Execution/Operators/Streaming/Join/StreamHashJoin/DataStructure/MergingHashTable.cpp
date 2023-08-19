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

#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/MergingHashTable.hpp>
#include <Util/Logger/Logger.hpp>
#include <atomic>

namespace NES::Runtime::Execution::Operators {
void MergingHashTable::insertBucket(size_t bucketPos, const FixedPagesLinkedList* pagesLinkedList) {
    //Note that this function creates a new Fixpage (that only moves ptr) instead of reusing the existing one

    auto& head = bucketHeads[bucketPos];
    auto& numItems = bucketNumItems[bucketPos];
    auto& numPages = bucketNumPages[bucketPos];

    for (auto&& page : pagesLinkedList->getPages()) {
        auto oldHead = head.load(std::memory_order::seq_cst);
        auto node = new InternalNode{std::make_unique<FixedPage>(page.get()), oldHead};
        while (!head.compare_exchange_weak(oldHead, node, std::memory_order::release, std::memory_order::seq_cst)) {
        }
        numItems.fetch_add(page->size(), std::memory_order::seq_cst);
    }
    numPages.fetch_add(pagesLinkedList->getPages().size(), std::memory_order::seq_cst);
}

//We should really think of unite this in the same layout as the other hash table
const std::vector<std::unique_ptr<FixedPage>> MergingHashTable::getPagesForBucket(size_t bucketPos) const {
    std::vector<std::unique_ptr<FixedPage>> ret;
    //Note we create a new vector here instead of giving out the pages
    ret.reserve(getNumPages(bucketPos));
    auto head = bucketHeads[bucketPos].load();
    while (head != nullptr) {
        auto* tmp = head;
        ret.insert(ret.begin(), std::move(tmp->dataPage));
        head = tmp->next;
    }

    return ret;
}

uint8_t* MergingHashTable::getTupleFromBucketAtPos(size_t bucketPos, size_t pageNo, size_t recordPos) {
    auto head = bucketHeads[bucketPos].load();
    //traverse list
    size_t pageCnt = 0;
    auto currentNode = head;
    while (pageCnt++ != pageNo) {
        NES_ASSERT(currentNode->next != nullptr, "Next node des not exists");
        currentNode = currentNode->next;
    }
    NES_ASSERT(currentNode->dataPage->size() > recordPos, "record does not exists");
    return currentNode->dataPage->getRecord(recordPos);
}

uint64_t MergingHashTable::getNumberOfTuplesForPage(size_t bucketPos, size_t pageNo) {
    auto head = bucketHeads[bucketPos].load();
    //traverse list
    size_t pageCnt = 0;
    auto currentNode = head;
    while (pageCnt++ != pageNo) {
        NES_ASSERT(currentNode->next != nullptr, "Next node des not exists");
        currentNode = currentNode->next;
    }

    return currentNode->dataPage->size();
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
        size_t pageCnt = 0;
        auto currentNode = bucket.load();
        while (pageCnt < bucketNumPages[pageCnt]) {
            ss << " pageNo=" << pageCnt++ << " ";
            ss << currentNode->dataPage->getContentAsString(schema);
            currentNode = currentNode->next;
        }
    }
    return ss.str();
}
}// namespace NES::Runtime::Execution::Operators
