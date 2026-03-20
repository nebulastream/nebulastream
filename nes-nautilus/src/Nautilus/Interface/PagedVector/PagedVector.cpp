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
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

PagedVector::Page PagedVector::Page::init(TupleBuffer buffer)
{
    Page tb(buffer);
    new (buffer.getAvailableMemoryArea<Header>().data()) Header(0);
    return tb;
}

PagedVector::Page PagedVector::Page::load(TupleBuffer buffer)
{
    return Page(buffer);
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

PagedVector PagedVector::init(TupleBuffer buffer)
{
    PagedVector pv(buffer);
    new (buffer.getAvailableMemoryArea<Header>().data()) Header(0);
    return pv;
}

PagedVector PagedVector::load(const TupleBuffer& buffer)
{
    PagedVector pv(buffer);
    return pv;
}

void PagedVector::updateCumulativeSumLastItem()
{
    if (isEmpty())
        return;
    const auto lastPageIndex = getNumberOfPages() - 1;
    uint64_t penultimateCumulativeSum = 0;
    if (lastPageIndex >= 1)
    {
        VariableSizedAccess::Index childBufferIndex{lastPageIndex - 1};
        auto childBuffer = buffer.loadChildBuffer(childBufferIndex);
        Page penultimatePage = Page::load(childBuffer);
        penultimateCumulativeSum = penultimatePage.getCumulativeSum();
    }
    VariableSizedAccess::Index lastBufferIndex{lastPageIndex};
    auto lastBuffer = buffer.loadChildBuffer(lastBufferIndex);
    Page lastPage = Page::load(lastBuffer);
    lastPage.setCumulativeSum(lastPage.buffer.getNumberOfTuples() + penultimateCumulativeSum);
}

void PagedVector::updateCumulativeSumAllPages()
{
    size_t curCumulativeSum = 0;
    uint64_t totalPages = getNumberOfPages();
    for (uint64_t i = 0; i < totalPages; i++)
    {
        VariableSizedAccess::Index pageChildIdx{i};
        auto pageBuffer = buffer.loadChildBuffer(pageChildIdx);
        Page page = Page::load(pageBuffer);
        page.setCumulativeSum(page.buffer.getNumberOfTuples() + curCumulativeSum);
        curCumulativeSum = page.getCumulativeSum();
    }
}

void PagedVector::movePagesFrom(PagedVector& other)
{
    addPages(other);
    updateCumulativeSumAllPages();
    /// if we really want to move, then we should change the numPages to 0. However, this breaks some tests & logic (or there's a bug).
    /// other.header().numPages = 0;
}

std::optional<uint64_t> PagedVector::getBufferPosForEntry(const uint64_t entryPos) const
{
    /// We need to find the index / page that the entryPos belongs to.
    return findIdx(entryPos).and_then(
        [&](const size_t index) -> std::optional<uint64_t>
        {
            /// We need to subtract the cumulative sum before our found index to get the position on the page
            uint64_t cumulativeSumBefore = 0;
            if (index > 0)
            {
                VariableSizedAccess::Index childBufferIndex{index - 1};
                auto childBuffer = buffer.loadChildBuffer(childBufferIndex);
                Page page = Page::load(childBuffer);
                cumulativeSumBefore = page.getCumulativeSum();
            }
            return entryPos - cumulativeSumBefore;
        });
}

uint64_t PagedVector::getBufferSize()
{
    return sizeof(Header);
}

const PagedVector::Page PagedVector::operator[](const size_t index) const
{
    VariableSizedAccess::Index childIndex{index};
    auto childBuffer = buffer.loadChildBuffer(childIndex);
    Page obj = Page::load(childBuffer);
    return obj;
}

void PagedVector::addExistingPage(TupleBuffer& newPageBuffer)
{
    auto newPageIdx = buffer.storeChildBuffer(newPageBuffer);
    header().numPages++;
}

void PagedVector::addNewPage(AbstractBufferProvider* bufferProvider, const uint64_t bufferSize)
{
    /// Either no pages exist, or the last page is full so allocate a new one
    if (auto page = bufferProvider->getUnpooledBuffer(bufferSize); page.has_value())
    {
        updateCumulativeSumLastItem();
        Page newPage = Page::init(page.value());
        auto newPageIdx = buffer.storeChildBuffer(page.value());
        header().numPages++;
    }
    else
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available!");
    }
}

void PagedVector::appendPageIfFull(AbstractBufferProvider* bufferProvider, const uint64_t capacity, const uint64_t bufferSize)
{
    PRECONDITION(bufferProvider != nullptr, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(capacity > 0, "At least one tuple has to fit on a page!");

    auto numPages = getNumberOfPages();
    if (numPages > 0)
    {
        VariableSizedAccess::Index lastPageIndex{numPages - 1};
        auto lastPage = buffer.loadChildBuffer(lastPageIndex);
        if (lastPage.getNumberOfTuples() < capacity)
        {
            return;
        }
    }
    /// Either no pages exist, or the last page is full so allocate a new one
    addNewPage(bufferProvider, bufferSize);
}

void PagedVector::addPages(const PagedVector& other)
{
    uint64_t totalPagesToAdd = other.getNumberOfPages();
    for (uint64_t i = 0; i < totalPagesToAdd; i++)
    {
        VariableSizedAccess::Index childIndex{i};
        auto childBuffer = other.buffer.loadChildBuffer(childIndex);
        addExistingPage(childBuffer);
    }
}

std::optional<size_t> PagedVector::findIdx(const uint64_t entryPos) const
{
    if (entryPos >= getTotalNumberOfEntries())
    {
        NES_WARNING("EntryPos {} exceeds the number of entries in the PagedVector {}!", entryPos, getTotalNumberOfEntries());
        return {};
    }

    const auto numPages = getNumberOfPages();

    for (size_t i = 0; i < numPages; ++i)
    {
        VariableSizedAccess::Index childBufferIndex{i};
        auto childBuffer = buffer.loadChildBuffer(childBufferIndex);
        Page page = Page::load(childBuffer);

        size_t effectiveCumulativeSum;
        if (i == numPages - 1)
        {
            /// As the cumulative sum on the last page might not have been updated since the
            /// last write operation, we need to use the number of tuples in the buffer instead
            uint64_t penultimateCumulativeSum = 0;
            if (numPages > 1)
            {
                VariableSizedAccess::Index penultimateIndex{i - 1};
                auto penultimateBuffer = buffer.loadChildBuffer(penultimateIndex);
                Page penultimatePage = Page::load(penultimateBuffer);
                penultimateCumulativeSum = penultimatePage.getCumulativeSum();
            }
            effectiveCumulativeSum = penultimateCumulativeSum + page.buffer.getNumberOfTuples() - 1;
        }
        else
        {
            effectiveCumulativeSum = page.getCumulativeSum() - 1;
        }

        if (entryPos <= effectiveCumulativeSum)
        {
            return i;
        }
    }
    return {};
}

uint64_t PagedVector::getTotalNumberOfEntries() const
{
    const auto numPages = getNumberOfPages();
    uint64_t penultimateCumulativeSum = 0;
    if (numPages > 1)
    {
        VariableSizedAccess::Index penultimateIndex{numPages - 2};
        auto penultimateBuffer = buffer.loadChildBuffer(penultimateIndex);
        Page penultimatePage = Page::load(penultimateBuffer);
        penultimateCumulativeSum = penultimatePage.getCumulativeSum();
    }
    uint64_t lastNumberOfTuples = 0;
    if (numPages > 0)
    {
        VariableSizedAccess::Index lastIndex{numPages - 1};
        auto lastBuffer = buffer.loadChildBuffer(lastIndex);
        Page lastPage = Page::load(lastBuffer);
        lastNumberOfTuples = lastPage.buffer.getNumberOfTuples();
    }
    return penultimateCumulativeSum + lastNumberOfTuples;
}

uint64_t PagedVector::getNumberOfPages() const
{
    return buffer.getAvailableMemoryArea<uint64_t>()[0];
}

uint64_t PagedVector::getNumberOfEntriesLastPage() const
{
    auto numPages = getNumberOfPages();
    if (numPages == 0)
    {
        return 0;
    }
    VariableSizedAccess::Index childIndex{numPages - 1};
    auto lastPage = buffer.loadChildBuffer(childIndex);
    return lastPage.getNumberOfTuples();
}

bool PagedVector::isEmpty() const
{
    if (buffer.getAvailableMemoryArea<uint64_t>()[0] > 0)
    {
        return false;
    }
    return true;
}
}
