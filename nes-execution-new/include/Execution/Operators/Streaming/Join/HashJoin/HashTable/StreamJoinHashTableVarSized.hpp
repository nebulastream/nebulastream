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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_STREAMJOINHASHTABLEVARSIZED_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_STREAMJOINHASHTABLEVARSIZED_HPP_

#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class represents a hash map that is not thread safe. It consists of multiple buckets each
 * consisting of a PagedVectorVarSized.
 */
class StreamJoinHashTableVarSized {
  public:
    /**
      * @brief Constructor for a StreamJoinHashTableVarSized
      * @param numPartitions
      * @param bufferManager
      * @param pageSize
      * @param schema
      */
    explicit StreamJoinHashTableVarSized(size_t numPartitions,
                                         BufferManagerPtr& bufferManager,
                                         size_t pageSize,
                                         SchemaPtr& schema);

    StreamJoinHashTableVarSized(const StreamJoinHashTableVarSized&) = delete;

    StreamJoinHashTableVarSized& operator=(const StreamJoinHashTableVarSized&) = delete;

    virtual ~StreamJoinHashTableVarSized() = default;

    /**
     * @brief Inserts the key into this hash table by returning a pointer to the appropriate underlying PagedVectorVarSized
     * @param key
     * @return pointer to the bucket
     */
    Nautilus::Interface::PagedVectorVarSizedPtr insert(uint64_t key) const;

    /**
     * @brief Calculates the bucket position for the hash
     * @param hash
     * @return bucket position
     */
    size_t getBucketPos(uint64_t hash) const;

    /**
     * @brief Returns the bucket at bucketPos
     * @param bucketPos
     * @return bucket
     */
    Nautilus::Interface::PagedVectorVarSizedPtr getBucketPagedVector(size_t bucketPos) const;

    /**
     * @brief Returns the number of tuples in the hash table
     * @return number of tuples
     */
    uint64_t getNumberOfTuples() const;

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
     * @brief Returns the number of buckets overall
     * @return number of buckets
     */
    size_t getNumBuckets() const;

  private:
    std::vector<Nautilus::Interface::PagedVectorVarSizedPtr> buckets;
    size_t mask;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_STREAMJOINHASHTABLEVARSIZED_HPP_
