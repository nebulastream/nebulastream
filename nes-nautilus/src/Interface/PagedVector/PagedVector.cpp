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
#include <Interface/PagedVector/PagedVector.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <tuple>
#include <utility>
#include <vector>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

void PagedVector::Page::init(TupleBuffer buffer)
{
    new (buffer.getAvailableMemoryArea<Header>().data()) Header(0);
}

PagedVector::Page PagedVector::Page::load(const TupleBuffer& buffer)
{
    return Page(buffer);
}

uint64_t PagedVector::Page::getHeaderSize()
{
    return sizeof(Header);
}

size_t PagedVector::Page::getCumulativeSum() const
{
    return header().cumulativeSum;
}

void PagedVector::Page::setCumulativeSum(size_t newCumulativeSum)
{
    header().cumulativeSum = newCumulativeSum;
}

uint64_t PagedVector::Page::getNumberOfTuples() const
{
    return buffer.getNumberOfTuples();
}

void PagedVector::init(TupleBuffer buffer, uint64_t pageBufferSize, uint64_t tupleSize, const uint64_t inlineSizeInBytes)
{
    auto* const header = new (buffer.getAvailableMemoryArea<Header>().data()) Header(0, pageBufferSize, tupleSize);
    buffer.setNumberOfTuples(0);
    if (buffer.getBufferSize() > sizeof(Header))
    {
        header->inlineCapacity = std::min<uint64_t>(inlineSizeInBytes, buffer.getBufferSize() - sizeof(Header)) / tupleSize;
    }
}

PagedVector PagedVector::load(const TupleBuffer& buffer)
{
    PagedVector pagedVector(buffer);
    return pagedVector;
}

void PagedVector::updateCumulativeSumLastItem() const
{
    if (getNumberOfPages() == 0)
    {
        return;
    }
    const auto lastPageIndex = getNumberOfPages() - 1;
    uint64_t penultimateCumulativeSum = 0;
    if (lastPageIndex >= 1)
    {
        auto childBuffer = buffer.loadChildBuffer(getPageChildIndex(lastPageIndex - 1));
        const Page penultimatePage = Page::load(childBuffer);
        penultimateCumulativeSum = penultimatePage.getCumulativeSum();
    }
    auto lastBuffer = buffer.loadChildBuffer(getPageChildIndex(lastPageIndex));
    Page lastPage = Page::load(lastBuffer);
    lastPage.setCumulativeSum(lastPage.getNumberOfTuples() + penultimateCumulativeSum);
}

void PagedVector::updateCumulativeSumAllPages() const
{
    size_t curCumulativeSum = 0;
    const uint64_t totalPages = getNumberOfPages();
    for (uint64_t i = 0; i < totalPages; i++)
    {
        auto pageBuffer = buffer.loadChildBuffer(getPageChildIndex(i));
        Page page = Page::load(pageBuffer);
        page.setCumulativeSum(page.getNumberOfTuples() + curCumulativeSum);
        curCumulativeSum = page.getCumulativeSum();
    }
}

void PagedVector::copyPagesFrom(AbstractBufferProvider& bufferProvider, const PagedVector& other)
{
    /// NOLINTNEXTLINE(readability-simplify-boolean-expr): clang-tidy sees the negated PRECONDITION expansion.
    PRECONDITION(
        header().status == VALID_PV and other.header().status == VALID_PV,
        "Both paged vectors have to be valid and properly initialized to copy pages.");
    PRECONDITION(header().inlineCapacity == 0 and other.header().inlineCapacity == 0, "Vectors with an inline page cannot copy pages.");
    const uint64_t numPagesToCopy = other.getNumberOfPages();

    for (uint64_t pageIdx = 0; pageIdx < numPagesToCopy; ++pageIdx)
    {
        auto sourcePage = other.buffer.loadChildBuffer(other.getPageChildIndex(pageIdx));
        auto destPage = deepCopyBuffer(sourcePage, bufferProvider);
        std::ignore = buffer.storeChildBuffer(destPage);
        header().numPages++;
    }
    header().numRecords += other.header().numRecords;
    updateCumulativeSumAllPages();
}

void PagedVector::movePagesFrom(PagedVector& other)
{
    /// NOLINTNEXTLINE(readability-simplify-boolean-expr): clang-tidy sees the negated PRECONDITION expansion.
    PRECONDITION(
        header().status == VALID_PV and other.header().status == VALID_PV,
        "Both paged vectors have to be valid and properly initialized to move pages.");
    PRECONDITION(header().inlineCapacity == 0 and other.header().inlineCapacity == 0, "Vectors with an inline page cannot move pages.");
    const uint64_t numPages = other.getNumberOfPages();
    for (uint64_t i = 0; i < numPages; ++i)
    {
        auto otherPageBuffer = other.buffer.loadChildBuffer(other.getPageChildIndex(i));
        std::ignore = buffer.storeChildBuffer(otherPageBuffer);
        header().numPages++;
    }
    header().numRecords += other.header().numRecords;
    updateCumulativeSumAllPages();
    other.header().status = INVALID_PV;
}

uint64_t PagedVector::getMainBufferSize(const uint64_t inlineSizeInBytes)
{
    return sizeof(Header) + inlineSizeInBytes;
}

void PagedVector::addNewPage(AbstractBufferProvider* bufferProvider, const uint64_t bufferSize)
{
    /// Either no pages exist, or the last page is full so allocate a new one
    auto page = getPooledBufferIfFits(bufferSize, *bufferProvider);
    updateCumulativeSumLastItem();
    Page::init(page);
    if (getNumberOfPages() == 0)
    {
        /// The children of the main buffer so far belong to the inline records.
        header().firstPageChildIndex = buffer.getNumberOfChildBuffers();
    }
    std::ignore = buffer.storeChildBuffer(page);
    header().numPages++;
}

void PagedVector::appendPageIfFull(AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(bufferProvider != nullptr, "BufferProvider can not be null.");
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for appending new pages.");
    if (getTotalNumberOfRecords() < getInlineCapacity())
    {
        /// The main buffer still has space
        return;
    }
    auto numPages = getNumberOfPages();
    if (numPages > 0)
    {
        auto lastPage = buffer.loadChildBuffer(getPageChildIndex(numPages - 1));
        if (lastPage.getNumberOfTuples() < getPageCapacity())
        {
            return;
        }
    }
    /// Either no pages exist, or the last page is full so allocate a new one.
    /// The page must be exactly as large as the capacity computed at init assumes.
    addNewPage(bufferProvider, getPageBufferSize());
}

size_t PagedVector::getPageIndex(uint64_t recordIndex) const
{
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for accessing one of its pages.");
    PRECONDITION(recordIndex < getTotalNumberOfRecords(), "Paged Vector out of bounds");
    PRECONDITION(recordIndex >= getInlineCapacity(), "The record is stored in the main buffer, not in a page");

    /// The pages only hold the records behind the inline ones.
    recordIndex -= getInlineCapacity();
    const auto numPages = getNumberOfPages();

    /// Page i covers records [prevCumSum, cumSum_i); we want the first page where cumSum > recordIndex.
    /// The last page's stored cumulativeSum is stale between writes (only refreshed by appendPageIfFull/updateCumulativeSum*),
    /// so for it we recompute the effective cumSum from penultimate + current numTuples.
    const auto effectiveCumulativeSum = [this, numPages](uint64_t pageIdx) -> uint64_t
    {
        const auto pageBuffer = buffer.loadChildBuffer(getPageChildIndex(pageIdx));
        const Page page = Page::load(pageBuffer);
        if (pageIdx + 1 < numPages)
        {
            return page.getCumulativeSum();
        }
        uint64_t penultimateCumulativeSum = 0;
        if (pageIdx > 0)
        {
            const auto penultimateBuffer = buffer.loadChildBuffer(getPageChildIndex(pageIdx - 1));
            penultimateCumulativeSum = Page::load(penultimateBuffer).getCumulativeSum();
        }
        return penultimateCumulativeSum + page.getNumberOfTuples();
    };

    const auto pageRange = std::views::iota(uint64_t{0}, numPages);
    /// NOLINTNEXTLINE(misc-include-cleaner): std::ranges::lower_bound lives in <algorithm>, which is already included above.
    const auto it = std::ranges::lower_bound(pageRange, recordIndex + 1, std::ranges::less{}, effectiveCumulativeSum);
    INVARIANT(it != pageRange.end(), "lower_bound failed despite recordIndex < getTotalNumberOfRecords()");
    return *it;
}

uint64_t PagedVector::getTotalNumberOfRecords() const
{
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for accessing total number of records.");
    return header().numRecords;
}

void PagedVector::incrementNumberOfRecords()
{
    header().numRecords++;
}

uint64_t PagedVector::getStatus() const
{
    return header().status;
}

uint64_t PagedVector::getNumberOfPages() const
{
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for accessing total number of records.");
    return header().numPages;
}

uint64_t PagedVector::getPageBufferSize() const
{
    return header().pageBufferSize;
}

uint64_t PagedVector::getTupleSize() const
{
    return header().tupleSize;
}

uint64_t PagedVector::getPageCapacity() const
{
    return header().pageCapacity;
}

uint64_t PagedVector::getInlineCapacity() const
{
    return header().inlineCapacity;
}

ChildBufferIndex PagedVector::getPageChildIndex(const uint64_t pageIndex) const
{
    return ChildBufferIndex{static_cast<uint32_t>(header().firstPageChildIndex + pageIndex)};
}

int8_t* PagedVector::locateRecord(const uint64_t recordIndex) const
{
    PRECONDITION(recordIndex < getTotalNumberOfRecords(), "Paged Vector out of bounds");
    const auto inlineCapacity = getInlineCapacity();
    if (recordIndex < inlineCapacity)
    {
        return getRecordAddress(buffer, recordIndex);
    }
    const auto pageIdx = getPageIndex(recordIndex);
    const auto page = buffer.loadChildBuffer(getPageChildIndex(pageIdx));
    uint64_t prevCumulativeSum = 0;
    if (pageIdx > 0)
    {
        const auto prevPage = buffer.loadChildBuffer(getPageChildIndex(pageIdx - 1));
        prevCumulativeSum = Page::load(prevPage).getCumulativeSum();
    }
    return getRecordAddress(page, (recordIndex - inlineCapacity) - prevCumulativeSum);
}

int8_t* PagedVector::reserveRecordSlot(AbstractBufferProvider* bufferProvider)
{
    appendPageIfFull(bufferProvider);
    const auto total = getTotalNumberOfRecords();
    const auto inlineCapacity = getInlineCapacity();
    int8_t* address = nullptr;
    if (total < inlineCapacity)
    {
        address = getRecordAddress(buffer, total);
        buffer.setNumberOfTuples(buffer.getNumberOfTuples() + 1);
    }
    else
    {
        const auto page = buffer.loadChildBuffer(getPageChildIndex(getNumberOfPages() - 1));
        address = getRecordAddress(page, (total - inlineCapacity) % getPageCapacity());
        page.setNumberOfTuples(page.getNumberOfTuples() + 1);
    }
    incrementNumberOfRecords();
    return address;
}

int8_t* PagedVector::getRecordAddress(const TupleBuffer& page, const uint64_t indexInPage) const
{
    /// The main buffer has the header of the paged vector in front of its records, the pages the header of a page.
    const auto* const pageStart = page.getAvailableMemoryArea<>().data();
    const auto* const mainStart = buffer.getAvailableMemoryArea<>().data();
    const uint64_t headerSize = pageStart == mainStart ? sizeof(Header) : Page::getHeaderSize();
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic)
    return reinterpret_cast<int8_t*>(const_cast<std::byte*>(pageStart)) + headerSize + (header().tupleSize * indexInPage);
}

bool PagedVector::isEmpty() const
{
    return getTotalNumberOfRecords() == 0;
}
}
