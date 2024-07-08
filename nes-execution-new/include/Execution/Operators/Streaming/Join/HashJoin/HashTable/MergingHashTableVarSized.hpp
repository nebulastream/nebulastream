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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_MERGINGHASHTABLEVARSIZED_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_MERGINGHASHTABLEVARSIZED_HPP_

#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class represents a hash map that is thread safe. It consists of multiple buckets each
 * consisting of a PagedVectorVarSized
 */
class MergingHashTableVarSized {
  public:
    /**
     * @brief Constructor for a hash table that supports simultaneous insertion with multiple threads
     * @param numBuckets
     */
    explicit MergingHashTableVarSized(size_t numBuckets);

    /**
     * @brief Inserts the pagedVector into the bucket at bucketPos
     * @param bucketPos
     * @param pagedVector
     */
    void insertBucket(size_t bucketPos, Nautilus::Interface::PagedVectorVarSizedPtr pagedVector);

    /**
     * @brief Returns the bucket at pos
     * @param bucket
     * @return reference to the PagedVector
     */
    void* getBucketAtPos(size_t bucket) const;

    /**
     * @brief Returns the number of tuples on the page in bucket at pos
     * @param bucket
     * @param page
     * @return number of tuples on the page
     */
    uint64_t getNumberOfTuplesForPage(size_t bucket, size_t page) const;

    /**
     * @brief Returns the number of items in the bucket at bucketPos
     * @param bucketPos
     * @return number of items in the bucket
     */
    size_t getNumItems(size_t bucketPos) const;

    /**
     * @brief Returns the number of pages in the bucket at bucketPos
     * @param bucketPos
     * @return number of pages in the bucket
     */
    size_t getNumPages(size_t bucketPos) const;

    /**
     * @brief Returns the number buckets overall
     * @return number of buckets
     */
    size_t getNumBuckets() const;

  private:
    std::vector<folly::Synchronized<Nautilus::Interface::PagedVectorVarSizedPtr>> bucketHeads;
    std::vector<std::atomic<size_t>> bucketNumItems;
    std::vector<std::atomic<size_t>> bucketNumPages;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_MERGINGHASHTABLEVARSIZED_HPP_
