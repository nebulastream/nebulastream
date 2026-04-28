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
#include <Runtime/VariableSizedAccess.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

void PagedVector::Page::init(TupleBuffer buffer)
{
    const Page page(buffer);
    new (buffer.getAvailableMemoryArea<Header>().data()) Header(0);
}

PagedVector::Page PagedVector::Page::load(TupleBuffer buffer)
{
    return Page(std::move(buffer));
}

uint64_t PagedVector::Page::getHeaderSize()
{
    return sizeof(Header);
}

size_t PagedVector::Page::getCumulativeSum()
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

PagedVector PagedVector::init(TupleBuffer buffer, uint64_t pageBufferSize, uint64_t tupleSize)
{
    PagedVector pagedVector(buffer);
    new (buffer.getAvailableMemoryArea<Header>().data()) Header(0, pageBufferSize, tupleSize);
    return pagedVector;
}

PagedVector PagedVector::load(const TupleBuffer& buffer)
{
    PagedVector pagedVector(buffer);
    return pagedVector;
}

void PagedVector::updateCumulativeSumLastItem() const
{
    if (isEmpty())
    {
        return;
    }
    const auto lastPageIndex = getNumberOfPages() - 1;
    uint64_t penultimateCumulativeSum = 0;
    if (lastPageIndex >= 1)
    {
        const VariableSizedAccess::Index childBufferIndex{lastPageIndex - 1};
        auto childBuffer = buffer.loadChildBuffer(childBufferIndex);
        Page penultimatePage = Page::load(childBuffer);
        penultimateCumulativeSum = penultimatePage.getCumulativeSum();
    }
    const VariableSizedAccess::Index lastBufferIndex{lastPageIndex};
    auto lastBuffer = buffer.loadChildBuffer(lastBufferIndex);
    Page lastPage = Page::load(lastBuffer);
    lastPage.setCumulativeSum(lastPage.getNumberOfTuples() + penultimateCumulativeSum);
}

void PagedVector::updateCumulativeSumAllPages() const
{
    size_t curCumulativeSum = 0;
    const uint64_t totalPages = getNumberOfPages();
    for (uint64_t i = 0; i < totalPages; i++)
    {
        const VariableSizedAccess::Index pageChildIdx{i};
        auto pageBuffer = buffer.loadChildBuffer(pageChildIdx);
        Page page = Page::load(pageBuffer);
        page.setCumulativeSum(page.getNumberOfTuples() + curCumulativeSum);
        curCumulativeSum = page.getCumulativeSum();
    }
}

void PagedVector::copyPagesFrom(AbstractBufferProvider& bufferProvider, const PagedVector& other)
{
    PRECONDITION(
        not(header().status != VALID_PV or other.header().status != VALID_PV),
        "Both paged vectors have to be valid and properly initialized to copy pages.");
    const uint64_t numPagesToCopy = other.getNumberOfPages();

    for (uint64_t pageIdx = 0; pageIdx < numPagesToCopy; ++pageIdx)
    {
        auto sourcePage = other.buffer.loadChildBuffer(VariableSizedAccess::Index{pageIdx});
        auto destPage = MemoryUtils::copyBuffer(sourcePage, bufferProvider);
        std::ignore = buffer.storeChildBuffer(destPage);
        header().numPages++;
    }
    updateCumulativeSumAllPages();
}

void PagedVector::movePagesFrom(PagedVector& other)
{
    PRECONDITION(
        not(header().status != VALID_PV or other.header().status != VALID_PV),
        "Both paged vectors have to be valid and properly initialized to move pages.");
    const uint64_t numPages = other.getNumberOfPages();
    for (uint64_t i = 0; i < numPages; ++i)
    {
        auto otherPageBuffer = other.buffer.loadChildBuffer(VariableSizedAccess::Index{i});
        std::ignore = buffer.storeChildBuffer(otherPageBuffer);
        header().numPages++;
    }
    updateCumulativeSumAllPages();
    other.header().status = INVALID_PV;
}

uint64_t PagedVector::getRecordIndexInPage(const uint64_t recordIndex) const
{
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for accessing a specific record relative to its page.");
    /// We need to find the index / page that the entryPos belongs to.

    auto index = getPageIndex(recordIndex);
    /// We need to subtract the cumulative sum before our found index to get the position on the page
    uint64_t cumulativeSumBefore = 0;
    if (index > 0)
    {
        const VariableSizedAccess::Index childBufferIndex{index - 1};
        auto childBuffer = buffer.loadChildBuffer(childBufferIndex);
        Page page = Page::load(childBuffer);
        cumulativeSumBefore = page.getCumulativeSum();
    }
    return recordIndex - cumulativeSumBefore;
}

uint64_t PagedVector::getMainBufferSize()
{
    return sizeof(Header);
}

PagedVector::Page PagedVector::operator[](const size_t index) const
{
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for accessing a specific record.");
    const VariableSizedAccess::Index childIndex{index};
    auto childBuffer = buffer.loadChildBuffer(childIndex);
    Page obj = Page::load(childBuffer);
    return obj;
}

void PagedVector::addNewPage(AbstractBufferProvider* bufferProvider, const uint64_t bufferSize)
{
    /// Either no pages exist, or the last page is full so allocate a new one
    if (auto page = bufferProvider->getUnpooledBuffer(bufferSize); page.has_value())
    {
        updateCumulativeSumLastItem();
        Page::init(page.value());
        std::ignore = buffer.storeChildBuffer(page.value());
        header().numPages++;
    }
    else
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available!");
    }
}

void PagedVector::appendPageIfFull(AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(bufferProvider != nullptr, "BufferProvider can not be null.");
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for appending new pages.");
    auto numPages = getNumberOfPages();
    if (numPages > 0)
    {
        const VariableSizedAccess::Index lastPageIndex{numPages - 1};
        auto lastPage = buffer.loadChildBuffer(lastPageIndex);
        if (lastPage.getNumberOfTuples() < getPageCapacity())
        {
            return;
        }
    }
    /// Either no pages exist, or the last page is full so allocate a new one
    addNewPage(bufferProvider, bufferProvider->getBufferSize());
}

size_t PagedVector::getPageIndex(const uint64_t recordIndex) const
{
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for accessing one of its pages.");
    PRECONDITION(recordIndex < getTotalNumberOfRecords(), "Paged Vector out of bounds");

    const auto numPages = getNumberOfPages();

    for (size_t i = 0; i < numPages; ++i)
    {
        const VariableSizedAccess::Index childBufferIndex{i};
        auto childBuffer = buffer.loadChildBuffer(childBufferIndex);
        Page page = Page::load(childBuffer);

        size_t effectiveCumulativeSum = 0;
        if (i == numPages - 1)
        {
            /// As the cumulative sum on the last page might not have been updated since the
            /// last write operation, we need to use the number of tuples in the buffer instead
            uint64_t penultimateCumulativeSum = 0;
            if (numPages > 1)
            {
                const VariableSizedAccess::Index penultimateIndex{i - 1};
                auto penultimateBuffer = buffer.loadChildBuffer(penultimateIndex);
                Page penultimatePage = Page::load(penultimateBuffer);
                penultimateCumulativeSum = penultimatePage.getCumulativeSum();
            }
            effectiveCumulativeSum = penultimateCumulativeSum + page.getNumberOfTuples() - 1;
        }
        else
        {
            effectiveCumulativeSum = page.getCumulativeSum() - 1;
        }

        if (recordIndex <= effectiveCumulativeSum)
        {
            return i;
        }
    }

    INVARIANT(false, "PagedVector out of bounds");
    std::unreachable();
}

uint64_t PagedVector::getTotalNumberOfRecords() const
{
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for accessing total number of records.");
    const auto numPages = getNumberOfPages();
    uint64_t penultimateCumulativeSum = 0;
    if (numPages > 1)
    {
        const VariableSizedAccess::Index penultimateIndex{numPages - 2};
        auto penultimateBuffer = buffer.loadChildBuffer(penultimateIndex);
        Page penultimatePage = Page::load(penultimateBuffer);
        penultimateCumulativeSum = penultimatePage.getCumulativeSum();
    }
    uint64_t lastNumberOfTuples = 0;
    if (numPages > 0)
    {
        const VariableSizedAccess::Index lastIndex{numPages - 1};
        auto lastBuffer = buffer.loadChildBuffer(lastIndex);
        const Page lastPage = Page::load(lastBuffer);
        lastNumberOfTuples = lastPage.getNumberOfTuples();
    }
    return penultimateCumulativeSum + lastNumberOfTuples;
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

uint64_t PagedVector::getNumberOfRecordsLastPage() const
{
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for accessing total number of records in the last page.");
    auto numPages = getNumberOfPages();
    if (numPages == 0)
    {
        return 0;
    }
    const VariableSizedAccess::Index childIndex{numPages - 1};
    auto lastPage = buffer.loadChildBuffer(childIndex);
    return lastPage.getNumberOfTuples();
}

bool PagedVector::isEmpty() const
{
    return getNumberOfPages() == 0;
}
}
