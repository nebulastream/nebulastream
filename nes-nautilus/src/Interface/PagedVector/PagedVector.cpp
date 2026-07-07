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

PagedVector::Page PagedVector::Page::load(TupleBuffer buffer)
{
    return Page(std::move(buffer));
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

void PagedVector::init(TupleBuffer buffer, uint64_t pageBufferSize, uint64_t tupleSize)
{
    new (buffer.getAvailableMemoryArea<Header>().data()) Header(0, pageBufferSize, tupleSize);
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
        const ChildBufferIndex childBufferIndex{static_cast<uint32_t>(lastPageIndex - 1)};
        auto childBuffer = buffer.loadChildBuffer(childBufferIndex);
        const Page penultimatePage = Page::load(childBuffer);
        penultimateCumulativeSum = penultimatePage.getCumulativeSum();
    }
    const ChildBufferIndex lastBufferIndex{static_cast<uint32_t>(lastPageIndex)};
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
        const ChildBufferIndex pageChildIdx{static_cast<uint32_t>(i)};
        auto pageBuffer = buffer.loadChildBuffer(pageChildIdx);
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
    const uint64_t numPagesToCopy = other.getNumberOfPages();

    for (uint64_t pageIdx = 0; pageIdx < numPagesToCopy; ++pageIdx)
    {
        auto sourcePage = other.buffer.loadChildBuffer(ChildBufferIndex{static_cast<uint32_t>(pageIdx)});
        auto destPage = deepCopyBuffer(sourcePage, bufferProvider);
        std::ignore = buffer.storeChildBuffer(destPage);
        header().numPages++;
    }
    updateCumulativeSumAllPages();
}

void PagedVector::movePagesFrom(PagedVector& other)
{
    /// NOLINTNEXTLINE(readability-simplify-boolean-expr): clang-tidy sees the negated PRECONDITION expansion.
    PRECONDITION(
        header().status == VALID_PV and other.header().status == VALID_PV,
        "Both paged vectors have to be valid and properly initialized to move pages.");
    const uint64_t numPages = other.getNumberOfPages();
    for (uint64_t i = 0; i < numPages; ++i)
    {
        auto otherPageBuffer = other.buffer.loadChildBuffer(ChildBufferIndex{static_cast<uint32_t>(i)});
        std::ignore = buffer.storeChildBuffer(otherPageBuffer);
        header().numPages++;
    }
    updateCumulativeSumAllPages();
    other.header().status = INVALID_PV;
}

uint64_t PagedVector::getMainBufferSize()
{
    return sizeof(Header);
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
        const ChildBufferIndex lastPageIndex{static_cast<uint32_t>(numPages - 1)};
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

    /// Page i covers records [prevCumSum, cumSum_i); we want the first page where cumSum > recordIndex.
    /// The last page's stored cumulativeSum is stale between writes (only refreshed by appendPageIfFull/updateCumulativeSum*),
    /// so for it we recompute the effective cumSum from penultimate + current numTuples.
    const auto effectiveCumulativeSum = [this, numPages](uint64_t pageIdx) -> uint64_t
    {
        const auto pageBuffer = buffer.loadChildBuffer(ChildBufferIndex{static_cast<uint32_t>(pageIdx)});
        const Page page = Page::load(pageBuffer);
        if (pageIdx + 1 < numPages)
        {
            return page.getCumulativeSum();
        }
        uint64_t penultimateCumulativeSum = 0;
        if (pageIdx > 0)
        {
            const auto penultimateBuffer = buffer.loadChildBuffer(ChildBufferIndex{static_cast<uint32_t>(pageIdx - 1)});
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
    const auto numPages = getNumberOfPages();
    uint64_t penultimateCumulativeSum = 0;
    if (numPages > 1)
    {
        const ChildBufferIndex penultimateIndex{static_cast<uint32_t>(numPages - 2)};
        auto penultimateBuffer = buffer.loadChildBuffer(penultimateIndex);
        const Page penultimatePage = Page::load(penultimateBuffer);
        penultimateCumulativeSum = penultimatePage.getCumulativeSum();
    }
    uint64_t lastNumberOfTuples = 0;
    if (numPages > 0)
    {
        const ChildBufferIndex lastIndex{static_cast<uint32_t>(numPages - 1)};
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

bool PagedVector::isEmpty() const
{
    return getNumberOfPages() == 0;
}
}
