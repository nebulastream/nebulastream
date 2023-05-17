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
#include <Nautilus/Interface/List/List.hpp>
#include <cstring>

namespace NES::Nautilus::Interface {

List::List(std::unique_ptr<std::pmr::memory_resource> allocator, uint64_t entrySize)
    : allocator(std::move(allocator)), entrySize(entrySize), totalNumberOfEntries(0) {
    appendPage();
    firstPage = &pages[0];
}

int8_t* List::appendPage() {
    auto page = reinterpret_cast<int8_t*>(allocator->allocate(PAGE_SIZE));
    pages.emplace_back(page);
    currentPage = page;
    numberOfEntries = 0;
    return page;
}

int8_t* List::getEntry(uint64_t pos) {
    auto pagePos = pos / capacityPerPage();
    auto positionOnPage = pos % capacityPerPage();

    return (pages[pagePos] + positionOnPage * entrySize);
}

List::~List() {
    for (auto* page : pages) {
        allocator->deallocate(page, PAGE_SIZE);
    }
}
size_t List::getNumberOfEntries() { return totalNumberOfEntries; }

size_t List::getNumberOfPages() { return pages.size(); }

size_t List::capacityPerPage() { return PAGE_SIZE / entrySize; }

const std::vector<int8_t*> List::getPages() { return pages; }

void List::moveTo(uint64_t oldPos, uint64_t newPos) {
    auto oldPosEntry = getEntry(oldPos);
    auto newPosEntry = getEntry(newPos);
    std::memcpy(newPosEntry, oldPosEntry, entrySize);
}

void List::clear() { pages.clear(); }

size_t List::getNumberOfEntriesOnCurrentPage() { return numberOfEntries; }

}// namespace NES::Nautilus::Interface