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
#ifndef NES_SHAREDJOINHASHTABLE_HPP
#define NES_SHAREDJOINHASHTABLE_HPP

#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/DataStructure/FixedPage.hpp>
#include <Execution/Operators/Streaming/Join/DataStructure/FixedPagesLinkedList.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Runtime/BloomFilter.hpp>
#include <atomic>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class represents a hash map that is thread safe. It consists of multiple buckets each
 * consisting of a linked list of FixedPages
 */
class SharedJoinHashTable {
  private:
    /**
     * @brief class that stores all pages for a single bucket
     */
    class InternalNode {
      public:
        FixedPage dataPage;
        std::atomic<InternalNode*> next{nullptr};
    };

  public:
    /**
     * @brief Constructor for a hash table that supports insertion simultaneously  of multiple threads
     * @param numBuckets
     */
    explicit SharedJoinHashTable(size_t numBuckets);

    /**
     * @brief inserts the pages into the bucket at the bucketPos
     * @param bucketPos
     * @param pagesLinkedList
     */
    void insertBucket(size_t bucketPos, FixedPagesLinkedList const* pagesLinkedList);

    /**
     * @brief returns all fixed pages
     * @param bucket
     * @return vector of fixed pages
     */
    std::vector<FixedPage> getPagesForBucket(size_t bucketPos) const;

    /**
     * @brief retrieves the number of items in the bucket
     * @param bucketPos
     * @return no. items of the bucket
     */
    size_t getNumItems(size_t bucketPos) const;

    /**
     * @brief Returns the number of pages belonging to the bucketPos
     * @param bucketPos
     * @return number of pages
     */
    size_t getNumPages(size_t bucketPos) const;

  private:
    std::vector<std::atomic<InternalNode*>> bucketHeads;
    std::vector<std::atomic<size_t>> bucketNumItems;
    std::vector<std::atomic<size_t>> bucketNumPages;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_SHAREDJOINHASHTABLE_HPP