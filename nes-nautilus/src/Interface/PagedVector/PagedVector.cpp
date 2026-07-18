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
#include <Interface/PagedVector/PagedVectorComparator.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <ranges>
#include <span>
#include <tuple>
#include <utility>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Util/Logger/Logger.hpp>
#include <Arena.hpp>
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
    /// initialize header through placement new
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
        const VariableSizedAccess::Index childBufferIndex{lastPageIndex - 1};
        auto childBuffer = buffer.loadChildBuffer(childBufferIndex);
        const Page penultimatePage = Page::load(childBuffer);
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
    /// NOLINTNEXTLINE(readability-simplify-boolean-expr): clang-tidy sees the negated PRECONDITION expansion.
    PRECONDITION(
        header().status == VALID_PV and other.header().status == VALID_PV,
        "Both paged vectors have to be valid and properly initialized to copy pages.");
    const uint64_t numPagesToCopy = other.getNumberOfPages();

    for (uint64_t pageIdx = 0; pageIdx < numPagesToCopy; ++pageIdx)
    {
        auto sourcePage = other.buffer.loadChildBuffer(VariableSizedAccess::Index{pageIdx});
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
        auto otherPageBuffer = other.buffer.loadChildBuffer(VariableSizedAccess::Index{i});
        std::ignore = buffer.storeChildBuffer(otherPageBuffer);
        header().numPages++;
    }
    updateCumulativeSumAllPages();
    other.header().status = INVALID_PV;
}

void PagedVector::stableSortRecords(const PagedVectorComparator& comparator, Arena& arena)
{
    PRECONDITION(getStatus() == VALID_PV, "Paged Vector must be valid for sorting.");

    const auto numberOfRecords = getTotalNumberOfRecords();
    if (numberOfRecords < 2)
    {
        return;
    }

    const auto tupleSize = getTupleSize();
    const auto numberOfPages = getNumberOfPages();
    PRECONDITION(tupleSize <= std::numeric_limits<size_t>::max() / numberOfRecords, "PagedVector sort allocation size overflow");

    const auto allocateAligned = [&arena](const size_t size, const size_t alignment) -> std::byte*
    {
        PRECONDITION(alignment > 0, "PagedVector sort allocation alignment must be positive");
        PRECONDITION(size <= std::numeric_limits<size_t>::max() - (alignment - 1), "PagedVector sort allocation size overflow");
        auto allocation = arena.allocateMemory(size + alignment - 1);
        void* allocationPointer = allocation.data();
        auto availableSize = allocation.size();
        auto* alignedPointer = std::align(alignment, size, allocationPointer, availableSize);
        INVARIANT(alignedPointer != nullptr, "Could not align PagedVector sort allocation");
        return static_cast<std::byte*>(alignedPointer);
    };

    /// Keep the page handles alive while sorting and merging. The handle array itself is arena-backed.
    auto* pages = reinterpret_cast<TupleBuffer*>( /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        allocateAligned(sizeof(TupleBuffer) * numberOfPages, alignof(TupleBuffer)));
    for (size_t pageIndex = 0; pageIndex < numberOfPages; ++pageIndex)
    {
        std::construct_at(pages + pageIndex, buffer.loadChildBuffer(VariableSizedAccess::Index{pageIndex}));
    }
    const auto pageDeleter = [numberOfPages](TupleBuffer* pageArray) { std::destroy_n(pageArray, numberOfPages); };
    const auto destroyPages = std::unique_ptr<TupleBuffer, decltype(pageDeleter)>{pages, pageDeleter};

    const auto recordAddress = [tupleSize](TupleBuffer& page, const uint64_t tupleIndex) -> int8_t*
    {
        auto* tuple = page.getAvailableMemoryArea<>().data() + Page::getHeaderSize() + (tupleIndex * tupleSize);
        return reinterpret_cast<int8_t*>(tuple); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    };
    const auto recordLess
        = [&comparator, &recordAddress](TupleBuffer& lhsPage, const uint64_t lhsIndex, TupleBuffer& rhsPage, const uint64_t rhsIndex)
    { return comparator.compare(&lhsPage, recordAddress(lhsPage, lhsIndex), &rhsPage, recordAddress(rhsPage, rhsIndex)); };

    /// Stable-sort every page independently. Bottom-up merge sort needs two index arrays and one reusable page-sized tuple scratch area.
    const auto pageCapacity = getPageCapacity();
    PRECONDITION(pageCapacity <= std::numeric_limits<size_t>::max() / sizeof(uint64_t), "PagedVector page sort allocation size overflow");
    auto* firstIndexes = reinterpret_cast<uint64_t*>( /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        allocateAligned(sizeof(uint64_t) * pageCapacity, alignof(uint64_t)));
    auto* secondIndexes = reinterpret_cast<uint64_t*>( /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        allocateAligned(sizeof(uint64_t) * pageCapacity, alignof(uint64_t)));
    auto* pageScratch = allocateAligned(tupleSize * pageCapacity, alignof(std::max_align_t));

    for (uint64_t pageIndex = 0; pageIndex < numberOfPages; ++pageIndex)
    {
        auto& page = pages[pageIndex];
        const auto numberOfPageRecords = page.getNumberOfTuples();
        if (numberOfPageRecords < 2)
        {
            continue;
        }
        std::iota(firstIndexes, firstIndexes + numberOfPageRecords, uint64_t{0});
        auto* sourceIndexes = firstIndexes;
        auto* destinationIndexes = secondIndexes;
        for (uint64_t runSize = 1; runSize < numberOfPageRecords; runSize *= 2)
        {
            for (uint64_t runBegin = 0; runBegin < numberOfPageRecords; runBegin += 2 * runSize)
            {
                auto left = runBegin;
                const auto leftEnd = std::min(runBegin + runSize, numberOfPageRecords);
                auto right = leftEnd;
                const auto rightEnd = std::min(runBegin + (2 * runSize), numberOfPageRecords);
                auto output = runBegin;
                while (left < leftEnd or right < rightEnd)
                {
                    if (left == leftEnd)
                    {
                        destinationIndexes[output++] = sourceIndexes[right++];
                    }
                    else if (right == rightEnd or not recordLess(page, sourceIndexes[right], page, sourceIndexes[left]))
                    {
                        /// Choosing the left run for equivalent records makes the page sort stable.
                        destinationIndexes[output++] = sourceIndexes[left++];
                    }
                    else
                    {
                        destinationIndexes[output++] = sourceIndexes[right++];
                    }
                }
            }
            std::swap(sourceIndexes, destinationIndexes);
        }

        for (uint64_t tupleIndex = 0; tupleIndex < numberOfPageRecords; ++tupleIndex)
        {
            std::memcpy(pageScratch + (tupleIndex * tupleSize), recordAddress(page, sourceIndexes[tupleIndex]), tupleSize);
        }
        std::memcpy(recordAddress(page, 0), pageScratch, numberOfPageRecords * tupleSize);
    }

    if (numberOfPages == 1)
    {
        return;
    }

    struct RunCursor
    {
        uint64_t pageIndex;
        uint64_t tupleIndex;
    };

    auto* heap = reinterpret_cast<RunCursor*>( /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        allocateAligned(sizeof(RunCursor) * numberOfPages, alignof(RunCursor)));
    size_t heapSize = 0;
    for (uint64_t pageIndex = 0; pageIndex < numberOfPages; ++pageIndex)
    {
        if (pages[pageIndex].getNumberOfTuples() > 0)
        {
            heap[heapSize++] = RunCursor{pageIndex, 0};
        }
    }

    /// std heap algorithms place the element for which this predicate is false against all others at the front. Returning true when lhs
    /// belongs after rhs therefore creates a min-heap. Page order breaks ties and preserves the PagedVector's original stable order.
    const auto belongsAfter = [&pages, &recordLess](const RunCursor& lhs, const RunCursor& rhs)
    {
        auto& lhsPage = pages[lhs.pageIndex];
        auto& rhsPage = pages[rhs.pageIndex];
        if (recordLess(rhsPage, rhs.tupleIndex, lhsPage, lhs.tupleIndex))
        {
            return true;
        }
        if (recordLess(lhsPage, lhs.tupleIndex, rhsPage, rhs.tupleIndex))
        {
            return false;
        }
        return lhs.pageIndex > rhs.pageIndex;
    };
    std::make_heap(heap, heap + heapSize, belongsAfter);

    auto* mergedRecords = allocateAligned(numberOfRecords * tupleSize, alignof(std::max_align_t));
    uint64_t outputIndex = 0;
    while (heapSize > 0)
    {
        std::pop_heap(heap, heap + heapSize, belongsAfter);
        auto cursor = heap[--heapSize];
        auto& sourcePage = pages[cursor.pageIndex];
        std::memcpy(mergedRecords + (outputIndex++ * tupleSize), recordAddress(sourcePage, cursor.tupleIndex), tupleSize);

        ++cursor.tupleIndex;
        if (cursor.tupleIndex < sourcePage.getNumberOfTuples())
        {
            heap[heapSize++] = cursor;
            std::push_heap(heap, heap + heapSize, belongsAfter);
        }
    }
    INVARIANT(outputIndex == numberOfRecords, "PagedVector merge produced {} records instead of {}", outputIndex, numberOfRecords);

    uint64_t mergedOffset = 0;
    for (uint64_t pageIndex = 0; pageIndex < numberOfPages; ++pageIndex)
    {
        auto& page = pages[pageIndex];
        const auto pageRecordsSize = page.getNumberOfTuples() * tupleSize;
        std::memcpy(recordAddress(page, 0), mergedRecords + mergedOffset, pageRecordsSize);
        mergedOffset += pageRecordsSize;
    }
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

    /// Page i covers records [prevCumSum, cumSum_i); we want the first page where cumSum > recordIndex.
    /// The last page's stored cumulativeSum is stale between writes (only refreshed by appendPageIfFull/updateCumulativeSum*),
    /// so for it we recompute the effective cumSum from penultimate + current numTuples.
    const auto effectiveCumulativeSum = [this, numPages](uint64_t pageIdx) -> uint64_t
    {
        const auto pageBuffer = buffer.loadChildBuffer(VariableSizedAccess::Index{pageIdx});
        const Page page = Page::load(pageBuffer);
        if (pageIdx + 1 < numPages)
        {
            return page.getCumulativeSum();
        }
        uint64_t penultimateCumulativeSum = 0;
        if (pageIdx > 0)
        {
            const auto penultimateBuffer = buffer.loadChildBuffer(VariableSizedAccess::Index{pageIdx - 1});
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
        const VariableSizedAccess::Index penultimateIndex{numPages - 2};
        auto penultimateBuffer = buffer.loadChildBuffer(penultimateIndex);
        const Page penultimatePage = Page::load(penultimateBuffer);
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

bool PagedVector::isEmpty() const
{
    return getNumberOfPages() == 0;
}
}
