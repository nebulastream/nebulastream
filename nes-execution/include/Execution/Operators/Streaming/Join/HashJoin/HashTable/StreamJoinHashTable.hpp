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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_STREAMJOINHASHTABLE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_STREAMJOINHASHTABLE_HPP_

#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/FixedPagesLinkedList.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/FixedPage/FixedPage.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Allocator/FixedPagesAllocator.hpp>
#include <atomic>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class represents a hash map that is not thread safe. It consists of multiple buckets each
 * consisting of a FixedPagesLinkedList.
 */
class StreamJoinHashTable {

  public:
    /**
     * @brief Constructor for a StreamJoinHashTable that
     * @param sizeOfRecord
     * @param numPartitions
     * @param fixedPagesAllocator
     * @param pageSize
     * @param preAllocPageSizeCnt
     */
    explicit StreamJoinHashTable(size_t sizeOfRecord,
                                 size_t numPartitions,
                                 FixedPagesAllocator& fixedPagesAllocator,
                                 size_t pageSize,
                                 size_t preAllocPageSizeCnt);

    StreamJoinHashTable(const StreamJoinHashTable&) = delete;

    StreamJoinHashTable& operator=(const StreamJoinHashTable&) = delete;

    virtual ~StreamJoinHashTable() = default;

    /**
     * @brief Inserts the key into this hash table by returning a pointer to a free memory space
     * @param key
     * @return Pointer to free memory space where the data shall be written
     */
    virtual uint8_t* insert(uint64_t key) const = 0;

    /**
     * @brief Returns the bucket at bucketPos
     * @param bucketPos
     * @return bucket
     */
    FixedPagesLinkedList* getBucketLinkedList(size_t bucketPos);

    /**
     * @brief Calculates the bucket position for the hash
     * @param hash
     * @return bucket position
     */
    size_t getBucketPos(uint64_t hash) const;

    /**
     * @brief debug mehtod to print the statistics of the hash table
     * @return
     */
    std::string getStatistics();

    /**
     * @brief get number of tuples in hash table
     * @return
     */
    uint64_t getNumberOfTuples();

    /**
     * @brief returns all fixed pages
     * @param bucket
     * @return vector of fixed pages
     */
    const std::vector<Nautilus::Interface::FixedPagePtr>& getPagesForBucket(size_t bucketPos) const;

    /**
     * @brief Returns the number of pages belonging to the bucketPos
     * @param bucketPos
     * @return number of pages
     */
    size_t getNumPages(size_t bucketPos) const;

    /**
     * @brief retrieves the number of items in the bucket
     * @param bucketPos
     * @return no. items of the bucket
     */
    size_t getNumItems(size_t bucketPos) const;

    /**
     * @brief retrieves the number of buckets overall
     * @return number of buckets
     */
    size_t getNumBuckets() const;

    /**
     * @brief this methods returnds the content of the page as a string
     * @return string
     */
    std::string getContentAsString(SchemaPtr schema) const;

  protected:
    std::vector<std::unique_ptr<FixedPagesLinkedList>> buckets;
    size_t mask;
    size_t numPartitions;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_STREAMJOINHASHTABLE_HPP_
