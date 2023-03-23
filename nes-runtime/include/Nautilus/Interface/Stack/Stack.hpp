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
#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACK_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACK_HPP_
#include <Runtime/Allocator/MemoryResource.hpp>
#include <cstdint>
#include <list>
#include <memory>
namespace NES::Nautilus::Interface {
class StackRef;
class Stack {
  public:
    static const uint64_t PAGE_SIZE = 4096;
    Stack(std::unique_ptr<std::pmr::memory_resource> allocator, uint64_t entrySize);
    int8_t* createChunk();
    size_t getNumberOfPages();
    const std::vector<int8_t*> getPages();
    void clear();
    size_t getNumberOfEntries();
    size_t capacityPerPage();
    size_t getNumberOfEntriesOnCurrentPage();
    ~Stack();

  private:
    friend StackRef;
    std::unique_ptr<std::pmr::memory_resource> allocator;
    uint64_t entrySize;
    std::vector<int8_t*> pages;
    int8_t* currentPage;
    uint64_t numberOfEntries;
};

}// namespace NES::Nautilus::Interface

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACK_HPP_
