#include <Nautilus/Interface/Stack/Stack.hpp>

namespace NES::Nautilus::Interface {

Stack::Stack(std::unique_ptr<std::pmr::memory_resource> allocator, uint64_t entrySize)
    : allocator(std::move(allocator)), entrySize(entrySize) {
    createChunk();
}

int8_t* Stack::createChunk() {
    auto page = reinterpret_cast<int8_t*>(allocator->allocate(PAGE_SIZE));
    pages.emplace_back(page);
    currentPage = page;
    numberOfEntries = 0;
    return page;
}

Stack::~Stack() {
    for (auto* page : pages) {
        allocator->deallocate(page, PAGE_SIZE);
    }
}
size_t Stack::getNumberOfEntries() { return (getNumberOfPages() - 1) * capacityPerPage() + numberOfEntries; }

size_t Stack::getNumberOfPages() {
    return pages.size();
}

size_t Stack::capacityPerPage() {
    return PAGE_SIZE / entrySize;
}

const std::vector<int8_t*> Stack::getPages() {
    return pages;
}

void Stack::clear() {
    pages.clear();
}

size_t Stack::getNumberOfEntriesOnCurrentPage() {
    return numberOfEntries;
}


}// namespace NES::Nautilus::Interface