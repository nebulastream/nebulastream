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
    int8_t* createChunk();

  private:
    friend StackRef;
    std::unique_ptr<std::pmr::memory_resource> allocator;
    std::vector<int8_t*> pages;
    int8_t* currentPage;
    uint64_t numberOfEntries;
    uint64_t entrySize;
};

}// namespace NES::Nautilus::Interface

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACK_HPP_
