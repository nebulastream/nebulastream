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
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>

namespace NES::Runtime::Execution::Operators {

uint8_t* FixedPagesLinkedList::append(const uint64_t hash) {
    uint8_t* retPointer = pages[pos]->append(hash);
    if (retPointer == nullptr) {
        if (++pos >= pages.size()) {
            auto ptr = fixedPagesAllocator.getNewPage(pageSize);
            pages.emplace_back(std::make_unique<FixedPage>(ptr, sizeOfRecord, pageSize));
        }
        retPointer = pages[pos]->append(hash);
    }

    return retPointer;
}

const std::vector<std::unique_ptr<FixedPage>>& FixedPagesLinkedList::getPages() const { return pages; }

FixedPagesLinkedList::FixedPagesLinkedList(FixedPagesAllocator& fixedPagesAllocator, size_t sizeOfRecord, size_t pageSize)
    : pos(0), fixedPagesAllocator(fixedPagesAllocator), sizeOfRecord(sizeOfRecord), pageSize(pageSize) {

    const auto numPreAllocatedPage = PREALLOCATED_SIZE / pageSize;
    NES_ASSERT2_FMT(numPreAllocatedPage > 0, "numPreAllocatedPage is 0");

    for (auto i = 0UL; i < numPreAllocatedPage; ++i) {
        auto ptr = fixedPagesAllocator.getNewPage(pageSize);
        pages.emplace_back(new FixedPage(ptr, sizeOfRecord, pageSize));
    }
}
}// namespace NES::Runtime::Execution::Operators