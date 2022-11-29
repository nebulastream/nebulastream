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
#ifndef NES_FIXEDPAGESLINKEDLIST_HPP
#define NES_FIXEDPAGESLINKEDLIST_HPP

#include <Execution/Operators/Streaming/Aggregations/Join/FixedPage.hpp>



namespace NES::Runtime::Execution::Operators {

/**
 * @brief
 */
class FixedPagesLinkedList{
  public:
    FixedPagesLinkedList(std::atomic<uint64_t>& tail, uint64_t overrunAddress, size_t sizeOfRecord);

    ~FixedPagesLinkedList();

    uint8_t* append(const uint64_t hash);

    const std::vector<FixedPage*>& getPages() const;

  private:
    std::atomic<uint64_t>& tail;
    FixedPage* curPage;
    size_t pos;
    uint64_t overrunAddress;
    std::vector<FixedPage*> pages;
    size_t sizeOfRecord;
};
}

#endif//NES_FIXEDPAGESLINKEDLIST_HPP
