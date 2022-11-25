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

#ifndef NES_LOCALHASHTABLE_HPP
#define NES_LOCALHASHTABLE_HPP

#include <atomic>

#include <API/Schema.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/FixedPage.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/FixedPagesLinkedList.hpp>

namespace NES::Runtime::Execution::Operators {
class LocalHashTable {

  public:
    explicit LocalHashTable(size_t sizeOfRecord,
                            size_t numPartitions,
                            std::atomic<uint64_t>& tail,
                            size_t overrunAddress);

    uint8_t* insert(uint64_t key) const;

    /**
     * @brief
     * @param bucketPos
     * @return
     */
    FixedPagesLinkedList* getBucketLinkedList(size_t bucketPos) const;

  private:
    std::vector<FixedPagesLinkedList*> buckets;


};
} // namespace NES::Runtime::Execution::Operators
#endif//NES_LOCALHASHTABLE_HPP
