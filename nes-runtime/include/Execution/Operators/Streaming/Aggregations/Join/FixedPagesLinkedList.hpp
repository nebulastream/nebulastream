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
    static constexpr auto PREALLOCATED_SIZE = 16 * 1024;
    static constexpr auto NUM_PREALLOCATED_PAGES = PREALLOCATED_SIZE / FixedPage::CHUNK_SIZE;

  public:
    void append(const uint64_t hash, Nautilus::Record& record);

    const std::vector<FixedPage*>& getPages() const;

  private:
    std::atomic<uint64_t>& tail;
    FixedPage* curPage;
    size_t pos;
    // TODO do we really need this overrunAddress
    uint64_t overrunAddress;
    std::vector<FixedPage*> pages;
};
}

#endif//NES_FIXEDPAGESLINKEDLIST_HPP
