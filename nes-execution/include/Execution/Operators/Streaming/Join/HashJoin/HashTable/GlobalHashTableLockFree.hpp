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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_GLOBALHASHTABLELOCKFREE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_GLOBALHASHTABLELOCKFREE_HPP_

#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/FixedPagesLinkedList.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/StreamJoinHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/FixedPage/FixedPage.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Allocator/FixedPagesAllocator.hpp>
#include <atomic>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class represents a hash map and ensures thread safety by compare and swaps
 */
class GlobalHashTableLockFree : public StreamJoinHashTable {

  public:
    /**
     * @brief Constructor for a GlobalHashTableLockFree that
     * @param sizeOfRecord
     * @param numPartitions
     * @param fixedPagesAllocator
     * @param pageSize
     * @param preAllocPageSizeCnt
     */
    explicit GlobalHashTableLockFree(size_t sizeOfRecord,
                                     size_t numPartitions,
                                     FixedPagesAllocator& fixedPagesAllocator,
                                     size_t pageSize,
                                     size_t preAllocPageSizeCnt);

    GlobalHashTableLockFree(const GlobalHashTableLockFree&) = delete;

    GlobalHashTableLockFree& operator=(const GlobalHashTableLockFree&) = delete;

    virtual ~GlobalHashTableLockFree() = default;

    /**
     * @brief Inserts the key into this hash table by returning a pointer to a free memory space
     * @param key
     * @return Pointer to free memory space where the data shall be written
     */
    virtual uint8_t* insert(uint64_t key) const override;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HASHTABLE_GLOBALHASHTABLELOCKFREE_HPP_
