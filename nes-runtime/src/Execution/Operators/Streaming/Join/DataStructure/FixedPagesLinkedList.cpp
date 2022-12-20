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
            auto ptr = tail.fetch_add(pageSize);
            NES_ASSERT2_FMT(ptr < overrunAddress, "Invalid address " << ptr << " < " << overrunAddress);
            pages.emplace_back(std::make_unique<FixedPage>(reinterpret_cast<uint8_t*>(ptr), sizeOfRecord, pageSize));
        }
        retPointer = pages[pos]->append(hash);
    }

    return retPointer;
}

const std::vector<std::unique_ptr<FixedPage>>& FixedPagesLinkedList::getPages() const { return pages; }

FixedPagesLinkedList::FixedPagesLinkedList(std::atomic<uint64_t>& tail, uint64_t overrunAddress,
                                           size_t sizeOfRecord, size_t pageSize)
    : tail(tail), pos(0), overrunAddress(overrunAddress), sizeOfRecord(sizeOfRecord), pageSize(pageSize) {

    const auto numPreAllocatedPage = 2; //PREALLOCATED_SIZE / pageSize;
    NES_ASSERT2_FMT(numPreAllocatedPage > 0, "numPreAllocatedPage is 0");

    for (auto i = 0UL; i < numPreAllocatedPage; ++i) {
        auto ptr = tail.fetch_add(pageSize);
        NES_ASSERT2_FMT(ptr < overrunAddress, "Invalid address " << ptr << " < " << overrunAddress);
        pages.emplace_back(new FixedPage(reinterpret_cast<uint8_t*>(ptr), sizeOfRecord, pageSize));
    }

}
} // namespace NES::Runtime::Execution::Operators