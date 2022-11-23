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

#include <Execution/Operators/Streaming/Aggregations/Join/FixedPagesLinkedList.hpp>

namespace NES::Runtime::Execution::Operators {

uint8_t* FixedPagesLinkedList::append(const uint64_t hash) {
    uint8_t* retPointer = curPage->append(hash);
    if (retPointer == nullptr) {
        if (++pos < pages.size()) {
            curPage = pages[pos];
        } else {
            pages.emplace_back(curPage = new FixedPage(this->tail, overrunAddress, sizeOfRecord));
        }
        retPointer = curPage->append(hash);
    }

    return retPointer;
}

const std::vector<FixedPage*>& FixedPagesLinkedList::getPages() const { return pages; }

FixedPagesLinkedList::FixedPagesLinkedList(std::atomic<uint64_t>& tail, uint64_t overrunAddress, size_t sizeOfRecord)
    : tail(tail), overrunAddress(overrunAddress), sizeOfRecord(sizeOfRecord) {
    for (auto i = 0; i < NUM_PREALLOCATED_PAGES; ++i) {
        pages.emplace_back(new FixedPage(this->tail, overrunAddress, sizeOfRecord));
    }
    curPage = pages[0];
}

FixedPagesLinkedList::~FixedPagesLinkedList() {
    std::for_each(pages.begin(), pages.end(), [](FixedPage* p) {
        delete p;
    });
}

} // namespace NES::Runtime::Execution::Operators