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
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::Runtime::Execution::Operators {

uint8_t* FixedPagesLinkedList::append(const uint64_t hash) {
    //try to insert on current Page
    uint8_t* retPointer = pages[pos]->append(hash);

    //check page is full
    if (retPointer == nullptr) {
        pageFullCnt++;
        if (++pos >= pages.size()) {
            //we need a new page
            allocateNewPageCnt++;
            auto ptr = fixedPagesAllocator.getNewPage(pageSize);
            pages.emplace_back(std::make_unique<FixedPage>(ptr, sizeOfRecord, pageSize));
        } else {
            //we still have an empty page left
            emptyPageStillExistsCnt++;
        }
        retPointer = pages[pos]->append(hash);
    }

    return retPointer;
}

uint8_t* FixedPagesLinkedList::appendConcurrent(const uint64_t hash) {
    //try to insert on current Page
    uint8_t* retPointer = pages[pos]->append(hash);
    if (retPointer != nullptr) {
        return retPointer;
    } else {
        size_t oldPos = pos;
        std::unique_lock<std::recursive_mutex> lock(pageAddMutex);
        if (pos != oldPos) {
            //this means somebody else already allocated a new page
            return appendConcurrent(hash);
        }

        if (pos + 1 >= pages.size()) {
            //we need a new page
            auto ptr = fixedPagesAllocator.getNewPage(pageSize);
            pages.emplace_back(std::make_unique<FixedPage>(ptr, sizeOfRecord, pageSize));
        } else {
            //there is a free page left
        }
        retPointer = pages[++pos]->append(hash);
    }

    return retPointer;
}

const std::vector<std::unique_ptr<FixedPage>>& FixedPagesLinkedList::getPages() const { return pages; }

FixedPagesLinkedList::FixedPagesLinkedList(FixedPagesAllocator& fixedPagesAllocator,
                                           size_t sizeOfRecord,
                                           size_t pageSize,
                                           size_t preAllocPageSizeCnt)
    : pos(0), fixedPagesAllocator(fixedPagesAllocator), sizeOfRecord(sizeOfRecord), pageSize(pageSize) {

    //pre allocate pages
    for (auto i = 0UL; i < preAllocPageSizeCnt; ++i) {
        auto ptr = fixedPagesAllocator.getNewPage(pageSize);
        pages.emplace_back(new FixedPage(ptr, sizeOfRecord, pageSize));
    }
}

std::string FixedPagesLinkedList::getStatistics() {
    std::stringstream ss;
    ss << "FixPagesLinkedList reports pageFullCnt=" << pageFullCnt << " allocateNewPageCnt=" << allocateNewPageCnt
       << " emptyPageStillExistsCnt=" << emptyPageStillExistsCnt;
    return ss.str();
}

}// namespace NES::Runtime::Execution::Operators