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

#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/FixedPagesLinkedList.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

uint8_t* FixedPagesLinkedList::appendLocal(const uint64_t hash) {
    //try to insert on current Page
    auto* retPointer = reinterpret_cast<uint8_t*>(pages[pos]->append(hash));
    //check page is full
    if (retPointer == nullptr) {
        pageFullCnt++;
        if (++pos >= pages.size()) {
            //we need a new page
            allocateNewPageCnt++;
            auto ptr = fixedPagesAllocator.getNewPage(pageSize);
            pages.emplace_back(std::make_unique<Nautilus::Interface::FixedPage>(ptr, sizeOfRecord, pageSize));
        } else {
            //we still have an empty page left
            emptyPageStillExistsCnt++;
        }
        retPointer = reinterpret_cast<uint8_t*>(pages[pos]->append(hash));
    }

    return retPointer;
}

uint8_t* FixedPagesLinkedList::appendConcurrentUsingLocking(const uint64_t hash) {
    const std::lock_guard<std::mutex> lock(pageAddMutex);
    return appendLocal(hash);
}

uint8_t* FixedPagesLinkedList::appendConcurrentLockFree(const uint64_t hash) {
    size_t oldPos = pos;
    //try to insert on current Page
    auto retPointer = reinterpret_cast<uint8_t*>(currentPage.load()->append(hash));
    if (retPointer != nullptr) {
        return retPointer;
    }

    //Insert was not successful so exactly one thread have to add a page
    bool expected = false;
    if (insertInProgress.compare_exchange_strong(expected, true, std::memory_order::release, std::memory_order::relaxed)) {
        // check if in the meantime somebody else already insert a page if so return
        if (pos != oldPos) {
            insertInProgress = false;
            return nullptr;
        }

        //check if there are pre-allocated pages left
        if (pos + 1 >= pages.size()) {
            //we need a new page
            auto ptr = fixedPagesAllocator.getNewPage(pageSize);
            pages.emplace_back(std::make_unique<Nautilus::Interface::FixedPage>(ptr, sizeOfRecord, pageSize));
            std::stringstream idAsString;
            idAsString << std::this_thread::get_id();
            NES_TRACE("Adding a new page for {} thread={}", hash, idAsString.str());
        } else {
            //there is a free page left
            std::stringstream idString;
            idString << std::this_thread::get_id();
            NES_TRACE("use existing page for {} thread={}", hash, idString.str());
        }

        //increment the position, this is atomic but maybe could also go without
        pos.fetch_add(1);

        //Make the new page the current page where threads try to insert
        auto oldPtr = pages[pos - 1].get();
        auto newPtr = pages[pos].get();
        while (!currentPage.compare_exchange_strong(oldPtr, newPtr, std::memory_order::release, std::memory_order::relaxed)) {
        }

        insertInProgress = false;
    }
    return nullptr;
}

const std::vector<Nautilus::Interface::FixedPagePtr>& FixedPagesLinkedList::getPages() const { return pages; }

FixedPagesLinkedList::FixedPagesLinkedList(FixedPagesAllocator& fixedPagesAllocator,
                                           size_t sizeOfRecord,
                                           size_t pageSize,
                                           size_t preAllocPageSizeCnt)
    : pos(0), fixedPagesAllocator(fixedPagesAllocator), sizeOfRecord(sizeOfRecord), pageSize(pageSize), insertInProgress(false) {

    NES_ASSERT2_FMT(preAllocPageSizeCnt >= 1, "We need at least one page preallocated");
    //pre allocate pages
    for (auto i = 0UL; i < preAllocPageSizeCnt; ++i) {
        auto ptr = fixedPagesAllocator.getNewPage(pageSize);
        //        pages.emplace_back(new FixedPage(ptr, sizeOfRecord, pageSize));
        pages.emplace_back(std::make_unique<Nautilus::Interface::FixedPage>(ptr, sizeOfRecord, pageSize));
    }
    currentPage = pages[0].get();
}

std::string FixedPagesLinkedList::getStatistics() {
    std::stringstream ss;
    ss << "FixPagesLinkedList reports pageFullCnt=" << pageFullCnt << " allocateNewPageCnt=" << allocateNewPageCnt
       << " emptyPageStillExistsCnt=" << emptyPageStillExistsCnt;
    return ss.str();
}

}// namespace NES::Runtime::Execution::Operators