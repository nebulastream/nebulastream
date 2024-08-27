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

#pragma once

#include <vector>
#include <Nautilus/Interface/FixedPage/FixedPage.hpp>
#include <Runtime/Allocator/FixedPagesAllocator.hpp>

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief This class implements a LinkedList consisting of FixedPages
 */
class FixedPagesLinkedList
{
public:
    /**
     * @brief Constructor for a FixedPagesLinkedList
     * @param fixedPagesAllocator
     * @param sizeOfRecord
     * @param pageSize
     */
    explicit FixedPagesLinkedList(
        Memory::FixedPagesAllocator& fixedPagesAllocator, size_t sizeOfRecord, size_t pageSize, size_t preAllocPageSizeCnt);

    /**
     * @brief Appends an item to this list by returning a pointer to a free memory space. This call is NOT thread safe
     * @return Pointer to a free memory space where to write the data
     */
    uint8_t* appendLocal();

    /**
     * @brief Appends an item to this list by returning a pointer to a free memory space. This call is thread safe and uses a mutex
     * @return Pointer to a free memory space where to write the data
     */
    uint8_t* appendConcurrentUsingLocking();

    /**
     * @brief Appends an item with the hash to this list by returning a pointer to a free memory space. This call is thread safe and is lockfree
     * @param hash
     * @return Pointer to a free memory space where to write the data
     */
    uint8_t* appendConcurrentLockFree(const uint64_t hash);

    /**
     * @brief Returns all pages belonging to this list
     * @return Vector containing pointer to the FixedPages
     */
    const std::vector<Nautilus::Interface::FixedPagePtr>& getPages() const;

    /**
     * @brief debug method to print the statistics of the Linked list
     */
    std::string getStatistics();

private:
    std::atomic<uint64_t> pos;
    Memory::FixedPagesAllocator& fixedPagesAllocator;
    std::vector<Nautilus::Interface::FixedPagePtr> pages;
    const size_t sizeOfRecord;
    const size_t pageSize;
    std::mutex pageAddMutex;

    ///used for printStatistics
    std::atomic<uint64_t> pageFullCnt = 0;
    std::atomic<uint64_t> allocateNewPageCnt = 0;
    std::atomic<uint64_t> emptyPageStillExistsCnt = 0;
    std::atomic<bool> insertInProgress;
    std::atomic<Nautilus::Interface::FixedPage*> currentPage;
};
} /// namespace NES::Runtime::Execution::Operators
