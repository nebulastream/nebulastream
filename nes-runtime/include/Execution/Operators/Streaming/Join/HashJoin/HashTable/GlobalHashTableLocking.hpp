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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_GLOBALHASHTABLELOCKING_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_GLOBALHASHTABLELOCKING_HPP_

#include <atomic>
#include <Nautilus/Interface/FixedPage/FixedPage.hpp>
#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/FixedPagesLinkedList.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/StreamJoinHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Allocator/FixedPagesAllocator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class represents a hash map and ensures thread safety by a mutex
 */
class GlobalHashTableLocking : public StreamJoinHashTable {

  public:
    /**
     * @brief Constructor for a GlobalHashTableLocking that
     * @param sizeOfRecord
     * @param numPartitions
     * @param fixedPagesAllocator
     * @param pageSize
     * @param preAllocPageSizeCnt
     */
    explicit GlobalHashTableLocking(size_t sizeOfRecord,
                                    size_t numPartitions,
                                    FixedPagesAllocator& fixedPagesAllocator,
                                    size_t pageSize,
                                    size_t preAllocPageSizeCnt);

    GlobalHashTableLocking(const GlobalHashTableLocking&) = delete;

    GlobalHashTableLocking& operator=(const GlobalHashTableLocking&) = delete;

    virtual ~GlobalHashTableLocking() = default;

    /**
     * @brief Inserts the key into this hash table by returning a pointer to a free memory space
     * @param key
     * @return Pointer to free memory space where the data shall be written
     */
    virtual uint8_t* insert(uint64_t key) const override;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_GLOBALHASHTABLELOCKING_HPP_
