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

#include <Execution/Operators/Streaming/Join/DataStructure/SharedJoinHashTable.hpp>
#include <atomic>

namespace NES::Runtime::Execution::Operators {
void SharedJoinHashTable::insertBucket(size_t bucketPos, const FixedPagesLinkedList* pagesLinkedList) {
    auto& head = bucketHeads[bucketPos];
    auto& numItems = bucketNumItems[bucketPos];
    auto& numPages = bucketNumPages[bucketPos];

    for (auto&& page : pagesLinkedList->getPages()) {
        auto oldHead = head.load(std::memory_order::relaxed);
        auto node = new InternalNode{FixedPage(page.get()), oldHead};
        while (!head.compare_exchange_weak(oldHead, node, std::memory_order::release, std::memory_order::relaxed)) {
        }
        numItems.fetch_add(page->size(), std::memory_order::relaxed);
    }
    numPages.fetch_add(pagesLinkedList->getPages().size(), std::memory_order::relaxed);
}

std::vector<FixedPage> SharedJoinHashTable::getPagesForBucket(size_t bucketPos) const {
    std::vector<FixedPage> ret;
    ret.reserve(getNumPages(bucketPos));
    auto head = bucketHeads[bucketPos].load();
    while (head != nullptr) {
        auto* tmp = head;
        ret.insert(ret.begin(), std::move(tmp->dataPage));
        head = tmp->next;
    }

    return ret;
}

size_t SharedJoinHashTable::getNumItems(size_t bucketPos) const { return bucketNumItems[bucketPos].load(); }

size_t SharedJoinHashTable::getNumPages(size_t bucketPos) const { return bucketNumPages[bucketPos].load(); }

SharedJoinHashTable::SharedJoinHashTable(size_t numBuckets)
    : bucketHeads(numBuckets), bucketNumItems(numBuckets), bucketNumPages(numBuckets) {}

}// namespace NES::Runtime::Execution::Operators
