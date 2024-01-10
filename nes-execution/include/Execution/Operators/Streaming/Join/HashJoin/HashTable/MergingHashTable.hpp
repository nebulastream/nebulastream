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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_MERGINGHASHTABLE_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_MERGINGHASHTABLE_HPP_

#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/FixedPagesLinkedList.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/FixedPage/FixedPage.hpp>
#include <Runtime/BloomFilter.hpp>
#include <atomic>
#include <folly/Synchronized.h>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class represents a hash map that is thread safe. It consists of multiple buckets each
 * consisting of a linked list of FixedPages
 */
class MergingHashTable {
  public:
    /**
     * @brief Constructor for a hash table that supports insertion simultaneously  of multiple threads
     * @param numBuckets
     */
    explicit MergingHashTable(size_t numBuckets);

    /**
     * @brief inserts the pages into the bucket at the bucketPos
     * @param bucketPos
     * @param pagesLinkedList
     */
    void insertBucket(size_t bucketPos, FixedPagesLinkedList const* pagesLinkedList);

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

    /**
     * @brief Returns the number buckets
     * @return number of buckets
     */
    size_t getNumBuckets() const;

    /**
     * @brief get the page at pos
     * @return pointer to page
     */
    Nautilus::Interface::FixedPage* getPageFromBucketAtPos(size_t bucket, size_t page);

    /**
     * @brief get number of tuples for a page
     * @return number of tuples
     */
    uint64_t getNumberOfTuplesForPage(size_t bucket, size_t page);

    /**
     * @brief this methods returnds the content of the page as a string
     * @return string
     */
    std::string getContentAsString(SchemaPtr schema) const;

  private:
    std::vector<folly::Synchronized<std::vector<Nautilus::Interface::FixedPagePtr>>> bucketHeads;
    std::vector<std::atomic<size_t>> bucketNumItems;
    std::vector<std::atomic<size_t>> bucketNumPages;
};
}// namespace NES::Runtime::Execution::Operators
#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_MERGINGHASHTABLE_HPP_
