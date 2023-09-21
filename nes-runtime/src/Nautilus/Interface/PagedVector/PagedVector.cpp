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
#include <Util/StdInt.hpp>
#include <cstring>

namespace NES::Nautilus::Interface {

PagedVector::PagedVector(std::unique_ptr<std::pmr::memory_resource> allocator, uint64_t entrySize, uint64_t pageSize)
    : allocator(std::move(allocator)), entrySize(entrySize), pageSize(pageSize), capacityPerPage(pageSize / entrySize),
      totalNumberOfEntries(0) {
    appendPage();
    NES_ASSERT2_FMT(entrySize > 0, "Entrysize for a pagedVector has to be larger than 0!");
    NES_ASSERT2_FMT(capacityPerPage > 0, "There has to fit at least one tuple on a page!");
}

int8_t* PagedVector::appendPage() {
    auto ptr = reinterpret_cast<int8_t*>(allocator->allocate(pageSize));
    pages.emplace_back(std::make_shared<FixedPage>(ptr, entrySize, pageSize));
    currentPage = pages[pages.size() - 1].get();
    return ptr;
}

int8_t* PagedVector::getEntry(uint64_t pos) const {

    auto pageNo = 0_u64;
    for (auto& page : pages) {
        if (pos < page->size()) {
            NES_DEBUG("Returning pointer to pageNo {} and posOnPage {}", pageNo, pos);
            return (*page)[pos];
        }

        pos -= page->size();
        ++pageNo;
    }

    // this return statement should only be reached if pos and totalNumberOfEntries are zero as we check prior to the loop that
    // pos does not exceed totalNumberOfEntries (provided currentPos of each FixedPage and totalNumberOfEntries are consistent)
    return (*pages[0])[0];
}

uint64_t PagedVector::getPageNo(uint64_t pos) const {
    NES_ASSERT2_FMT(pos == 0 || pos < totalNumberOfEntries, "Entry does not exist");

    for (auto pageNo = 0UL; pageNo < pages.size(); ++pageNo) {
        auto& page = pages[pageNo];
        if (pos < page->size()) {
            NES_DEBUG("Returning pageNo {} for pos {}", pageNo, pos);
            return pageNo;
        }

        pos -= page->size();
    }

    // this return statement should only be reached if pos and totalNumberOfEntries are zero as we check prior to the loop that
    // pos does not exceed totalNumberOfEntries (provided currentPos of each FixedPage and totalNumberOfEntries are consistent)
    return 0;
}

PagedVector::~PagedVector() {
    for (auto& page : pages) {
        allocator->deallocate((*page)[0], pageSize);
    }
}

uint64_t PagedVector::getTotalNumberOfEntries() const { return totalNumberOfEntries; }

void PagedVector::setTotalNumberOfEntries(uint64_t entries) { totalNumberOfEntries = entries; }

uint64_t PagedVector::getNumberOfPages() { return pages.size(); }

uint64_t PagedVector::getCapacityPerPage() const { return capacityPerPage; }

std::vector<FixedPagePtr>& PagedVector::getPages() { return pages; }

void PagedVector::moveFromTo(uint64_t oldPos, uint64_t newPos) const {
    auto oldPosEntry = getEntry(oldPos);
    auto newPosEntry = getEntry(newPos);
    std::memcpy(newPosEntry, oldPosEntry, entrySize);
    NES_DEBUG("Moved from {} to {}", oldPos, newPos);
}

void PagedVector::clear() { pages.clear(); }

uint64_t PagedVector::getNumberOfEntriesOnCurrentPage() const { return currentPage->size(); }

void PagedVector::moveAllPages(PagedVector& other) {
    NES_ASSERT2_FMT(pageSize == other.pageSize, "Can not combine PagedVector of different pageSizes for now!");
    NES_ASSERT2_FMT(entrySize == other.entrySize, "Can not combine PagedVector of different entrySize for now!");

    pages.insert(pages.end(), other.pages.begin(), other.pages.end());
    totalNumberOfEntries += other.totalNumberOfEntries;
    currentPage = pages[pages.size() - 1].get();
    other.pages.clear();
}

uint64_t PagedVector::getPageSize() const { return pageSize; }

int8_t* PagedVector::getFixedPage(const uint64_t& pageNo) {
    NES_DEBUG("Returning fixedPage for pageNo {} out of {} pages", pageNo, pages.size());
    return reinterpret_cast<int8_t*>(pages[pageNo].get());
}
}// namespace NES::Nautilus::Interface