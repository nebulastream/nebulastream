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
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>

namespace NES::Nautilus::Interface {

PagedVector::PagedVector(std::unique_ptr<std::pmr::memory_resource> allocator, uint64_t entrySize, uint64_t pageSize)
    : allocator(std::move(allocator)), entrySize(entrySize), pageSize(pageSize), numberOfEntries(0), totalNumberOfEntries(0) {
    appendPage();
    NES_ASSERT2_FMT(entrySize > 0, "Entrysize for a pagedVector has to be larger than 0!");
    NES_ASSERT2_FMT(capacityPerPage() > 0, "There has to fit at least one tuple on a page!");
}

int8_t* PagedVector::appendPage() {
    auto page = reinterpret_cast<int8_t*>(allocator->allocate(pageSize));
    pages.emplace_back(page);
    currentPage = page;
    numberOfEntries = 0;
    return page;
}

int8_t* PagedVector::getEntry(uint64_t pos) {
    auto pagePos = pos / capacityPerPage();
    auto positionOnPage = pos % capacityPerPage();

    return (pages[pagePos] + positionOnPage * entrySize);
}

PagedVector::~PagedVector() {
    for (auto* page : pages) {
        allocator->deallocate(page, pageSize);
    }
}

size_t PagedVector::getNumberOfEntries() { return totalNumberOfEntries; }

void PagedVector::setNumberOfEntries(size_t entries) { totalNumberOfEntries = entries; }

size_t PagedVector::getNumberOfPages() { return pages.size(); }

size_t PagedVector::capacityPerPage() { return pageSize / entrySize; }

const std::vector<int8_t*> PagedVector::getPages() { return pages; }

void PagedVector::moveFromTo(uint64_t oldPos, uint64_t newPos) {
    auto oldPosEntry = getEntry(oldPos);
    auto newPosEntry = getEntry(newPos);
    std::memcpy(newPosEntry, oldPosEntry, entrySize);
    NES_DEBUG("Moved from {} to {}", oldPos, newPos);
}

void PagedVector::clear() { pages.clear(); }

size_t PagedVector::getNumberOfEntriesOnCurrentPage() { return numberOfEntries; }

void PagedVector::combinePagedVectors(const PagedVector& other) {
    NES_ASSERT2_FMT(pageSize == other.pageSize, "Can not combine PagedVector of different pageSizes");

    pages.reserve(pages.size() + other.pages.size());
    for (auto& page : other.pages) {
        pages.emplace_back(page);
    }

    currentPage = pages[pages.size() - 1];
    numberOfEntries = other.numberOfEntries;
    totalNumberOfEntries += other.totalNumberOfEntries;
}
uint64_t PagedVector::getEntrySize() const {
    return entrySize;
}
uint64_t PagedVector::getPageSize() const {
    return pageSize;
}

}// namespace NES::Nautilus::Interface