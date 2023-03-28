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
#include <Nautilus/Interface/Stack/Stack.hpp>

namespace NES::Nautilus::Interface {

Stack::Stack(std::unique_ptr<std::pmr::memory_resource> allocator, uint64_t entrySize)
    : allocator(std::move(allocator)), entrySize(entrySize) {
    appendPage();
}

int8_t* Stack::appendPage() {
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