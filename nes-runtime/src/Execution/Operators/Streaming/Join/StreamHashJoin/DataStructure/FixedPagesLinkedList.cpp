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
    std::unique_lock lock(pageAddMutex);
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
    size_t oldPos = pos;
    //try to insert on current Page
    auto retPointer = currentPage.load()->append(hash);
    if (retPointer != nullptr) {
        return retPointer;
    }

    //one thread wins and do the replacement
    bool expected = false;
    if (insertInProgress.compare_exchange_strong(expected, true, std::memory_order::release, std::memory_order::relaxed)) {
        // check if in the meantime somebody else alrady did it
        if(pos != oldPos)
        {
            NES_ERROR("somebody else inserted already " << hash << " thread=" << std::this_thread::get_id());
            insertInProgress = false;
            return nullptr;
        }

        if (pos + 1 >= pages.size()) {
            NES_ERROR("create new page " << hash << " thread=" << std::this_thread::get_id());
            //we need a new page
            auto ptr = fixedPagesAllocator.getNewPage(pageSize);
            pages.emplace_back(std::make_unique<FixedPage>(ptr, sizeOfRecord, pageSize));
        } else {
            //there is a free page left
            NES_ERROR("use existing page " << hash << " thread=" << std::this_thread::get_id());
        }
        //TODO this does not have to be amtoic anmore
        pos.fetch_add(1);

        FixedPage* oldPtr = pages[pos - 1].get();
        FixedPage* newPtr = pages[pos].get();
        while (!currentPage.compare_exchange_strong(oldPtr, newPtr, std::memory_order::release, std::memory_order::relaxed)) {
        }
        insertInProgress = false;
    }
    return nullptr;
}


//uint8_t* FixedPagesLinkedList::appendConcurrent(const uint64_t hash) {
//    std::unique_lock lock(pageAddMutex);
//    //try to insert on current Page
//    uint8_t* retPointer = pages[pos]->append(hash);
//
//    //check page is full
//    if (retPointer == nullptr) {
//        pageFullCnt++;
//        if (++pos >= pages.size()) {
//            //we need a new page
//            allocateNewPageCnt++;
//            auto ptr = fixedPagesAllocator.getNewPage(pageSize);
//            pages.emplace_back(std::make_unique<FixedPage>(ptr, sizeOfRecord, pageSize));
//        } else {
//            //we still have an empty page left
//            emptyPageStillExistsCnt++;
//        }
//        retPointer = pages[pos]->append(hash);
//    }
//
//    return retPointer;
//}
//
//uint8_t* FixedPagesLinkedList::appendConcurrent(const uint64_t hash) {
//    //try to insert on current Page
//    size_t oldPos = pos;
//    uint8_t* retPointer = nullptr;
//    if (pages[pos] != nullptr) {
//        retPointer = pages[pos]->append(hash);
//    } else {
//        return nullptr;
//    }
//
//    if (retPointer == nullptr) {
//        bool expected = false;
//        if (insertInProgress.compare_exchange_strong(expected, true, std::memory_order::release, std::memory_order::relaxed)) {
//            if (oldPos != pos) {
//                NES_ERROR("something happened in the meantime " << hash << " thread=" << std::this_thread::get_id());
//                insertInProgress = false;
//                return nullptr;
//            }
//            //only one thread wins and does the following
//            if (pos + 1 >= pages.size()) {
//                NES_ERROR("create new page " << hash << " thread=" << std::this_thread::get_id());
//                //we need a new page
//                auto ptr = fixedPagesAllocator.getNewPage(pageSize);
//                pages.emplace_back(std::make_unique<FixedPage>(ptr, sizeOfRecord, pageSize));
//            } else {
//                //there is a free page left
//                NES_ERROR("use existing page " << hash << " thread=" << std::this_thread::get_id());
//            }
//
//            //this thread is the first to insert on the new page so it is guaranteed to succeed
//            retPointer = pages[pos + 1]->append(hash);
//            pos++;
//            insertInProgress = false;
//            return retPointer;
//        } else {
//            //all other threads spin here to wait until the new page is added and they can access it
//            while (insertInProgress) {
//            }
//        }
//    } else {
//        //got valid pointer
//        return retPointer;
//    }
//    return nullptr;
//}
//
//
//uint8_t* FixedPaesLinkedList::appendConcurrent(const uint64_t hash) {
//    //try to insert on current Page
//    size_t oldPos = pos;
//    //    NES_ERROR("try insert page pos " << pos << hash << " thread=" << std::this_thread::get_id());
//    uint8_t* retPointer = pages[pos]->append(hash);
//    if (retPointer != nullptr) {
//        //        NES_ERROR("Return valid Pointer " << hash);
//        return retPointer;
//    }
//
//    if (insertInProgress) {
//        return nullptr;
//    }
//
//    //this means the current page is full
//    bool expected = false;
//    //One thread must insert
//    if (insertInProgress.compare_exchange_strong(expected, true, std::memory_order::release, std::memory_order::relaxed)) {
//        if (pos + 1 >= pages.size()) {
//            NES_ERROR("create new page " << hash << " thread=" << std::this_thread::get_id());
//
//            //we need a new page
//            auto ptr = fixedPagesAllocator.getNewPage(pageSize);
//            pages.emplace_back(std::make_unique<FixedPage>(ptr, sizeOfRecord, pageSize));
//        } else {
//            //there is a free page left
//            NES_ERROR("use existing page " << hash << " thread=" << std::this_thread::get_id());
//        }
//
//        pos++;
//        insertInProgress = false;
//        return nullptr;
//    } else {
//        //the looser just returns
//        return nullptr;
//    }
//}

const folly::fbvector<std::unique_ptr<FixedPage>>& FixedPagesLinkedList::getPages() const { return pages; }

FixedPagesLinkedList::FixedPagesLinkedList(FixedPagesAllocator& fixedPagesAllocator,
                                           size_t sizeOfRecord,
                                           size_t pageSize,
                                           size_t preAllocPageSizeCnt)
    : pos(0), fixedPagesAllocator(fixedPagesAllocator), sizeOfRecord(sizeOfRecord), pageSize(pageSize), insertInProgress(false) {

    NES_ASSERT2_FMT(preAllocPageSizeCnt >= 1, "We need at least one page preallocated");
    //pre allocate pages
    for (auto i = 0UL; i < preAllocPageSizeCnt; ++i) {
        auto ptr = fixedPagesAllocator.getNewPage(pageSize);
        pages.emplace_back(new FixedPage(ptr, sizeOfRecord, pageSize));
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