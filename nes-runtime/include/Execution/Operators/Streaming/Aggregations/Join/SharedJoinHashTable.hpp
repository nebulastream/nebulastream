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
#include <atomic>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class implements a shared hash table for one of the join sides
 *
 */
class SharedJoinHashTable {
  private:
    /**
     * @brief class that stores the pages for a single bucket
     */
    class InternalNode {
        ImmutableFixedPage data;
        std::atomic<InternalNode*> next;

      private:

    };


  private:
    std::vector<std::atomic<InternalNode*>> bucketHeads;
    std::vector<std::atomic<size_t>> bucketNumItems;
    std::vector<std::atomic<size_t>> bucketNumPages;
    SchemaPtr schema;
};





} // namespace NES::Runtime::Execution::Operators
#endif//NES_SHAREDJOINHASHTABLE_HPP