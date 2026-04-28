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
#include <google/protobuf/stubs/port.h>
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

PagedVector PagedVector::init(TupleBuffer buffer, uint64_t pageBufferSize, uint64_t tupleSize)
{
    PagedVector pv(buffer);
    new (buffer.getAvailableMemoryArea<Header>().data()) Header(0, pageBufferSize, tupleSize);
    return pv;
}

PagedVector PagedVector::load(const TupleBuffer& buffer)
{
    PagedVector pv(buffer);
    return pv;
}

void PagedVector::updateCumulativeSumLastItem() const
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

void PagedVector::updateCumulativeSumAllPages() const
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

void PagedVector::copyPagesFrom(AbstractBufferProvider* bufferProvider, const PagedVector& other)
{
    INVARIANT(bufferProvider != nullptr, "BufferProvider must not be null");
    PRECONDITION(
        header().status == VALID_PV and other.header().status == VALID_PV,
        "Both paged vectors have to be valid and properly initialized to copy pages.");
    const uint64_t numPagesToCopy = other.getNumberOfPages();

    for (uint64_t pageIdx = 0; pageIdx < numPagesToCopy; ++pageIdx)
    {
        auto sourcePage = other.buffer.loadChildBuffer(VariableSizedAccess::Index{pageIdx});
        auto destPageOpt = bufferProvider->getUnpooledBuffer(sourcePage.getBufferSize());
        if (!destPageOpt.has_value())
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for deep copy of page at index={}", pageIdx);
        }
        auto destPage = destPageOpt.value();
        sourcePage.deepCopy(destPage);

        const uint64_t numVarSizedChildren = sourcePage.getNumberOfChildBuffers();
        for (uint64_t childIdx = 0; childIdx < numVarSizedChildren; ++childIdx)
        {
            auto sourceVarSizedChild = sourcePage.loadChildBuffer(VariableSizedAccess::Index{childIdx});
            auto destVarSizedChildOpt = bufferProvider->getUnpooledBuffer(sourceVarSizedChild.getBufferSize());
            if (!destVarSizedChildOpt.has_value())
            {
                throw BufferAllocationFailure(
                    "No unpooled TupleBuffer available for deep copy of var-sized child at page={}, child={}", pageIdx, childIdx);
            }
            auto destVarSizedChild = destVarSizedChildOpt.value();
            sourceVarSizedChild.deepCopy(destVarSizedChild);
            auto varSizedChildIdx = destPage.storeChildBuffer(destVarSizedChild);
        }

        auto pageChildIdx = buffer.storeChildBuffer(destPage);
        header().numPages++;
    }
    updateCumulativeSumAllPages();
}

void PagedVector::movePagesFrom(PagedVector& other)
{
    PRECONDITION(
        header().status == VALID_PV and other.header().status == VALID_PV,
        "Both paged vectors have to be valid and properly initialized to move pages.");
    uint64_t numPages = other.getNumberOfPages();
    for (uint64_t i = 0; i < numPages; ++i)
    {
        auto otherPageBuffer = other.buffer.loadChildBuffer(VariableSizedAccess::Index{i});
        auto childBufferIndex = buffer.storeChildBuffer(otherPageBuffer);
        header().numPages++;
    }
    updateCumulativeSumAllPages();
    other.header().status = INVALID_PV;
}

std::optional<uint64_t> PagedVector::getRecordIndexInPage(const uint64_t entryPos) const
{
    PRECONDITION(
        getStatus() == PagedVector::PagedVectorStatus::VALID_PV,
        "Paged Vector must be valid for accessing a specific record relative to its page.");
    /// We need to find the index / page that the entryPos belongs to.
    return getPageIndex(entryPos).and_then(
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

uint64_t PagedVector::getMainBufferSize()
{
    return sizeof(Header);
}

const PagedVector::Page PagedVector::operator[](const size_t index) const
{
    PRECONDITION(getStatus() == PagedVector::PagedVectorStatus::VALID_PV, "Paged Vector must be valid for accessing a specific record.");
    VariableSizedAccess::Index childIndex{index};
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
        Page newPage = Page::init(page.value());
        auto newPageIdx = buffer.storeChildBuffer(page.value());
        header().numPages++;
    }
    else
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available!");
    }
}

void PagedVector::appendPageIfFull(AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(bufferProvider != nullptr, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(getStatus() == PagedVector::PagedVectorStatus::VALID_PV, "Paged Vector must be valid for appending new pages.");
    auto numPages = getNumberOfPages();
    if (numPages > 0)
    {
        VariableSizedAccess::Index lastPageIndex{numPages - 1};
        auto lastPage = buffer.loadChildBuffer(lastPageIndex);
        if (lastPage.getNumberOfTuples() < getPageCapacity())
        {
            return;
        }
    }
    /// Either no pages exist, or the last page is full so allocate a new one
    addNewPage(bufferProvider, bufferProvider->getBufferSize());
}

std::optional<size_t> PagedVector::getPageIndex(const uint64_t entryPos) const
{
    PRECONDITION(getStatus() == PagedVector::PagedVectorStatus::VALID_PV, "Paged Vector must be valid for accessing one of its pages.");
    if (entryPos >= getTotalNumberOfRecords())
    {
        NES_WARNING("EntryPos {} exceeds the number of entries in the PagedVector {}!", entryPos, getTotalNumberOfRecords());
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

uint64_t PagedVector::getTotalNumberOfRecords() const
{
    PRECONDITION(
        getStatus() == PagedVector::PagedVectorStatus::VALID_PV, "Paged Vector must be valid for accessing total number of records.");
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

PagedVector::PagedVectorStatus PagedVector::getStatus() const
{
    return header().status;
}

uint64_t PagedVector::getNumberOfPages() const
{
    PRECONDITION(
        getStatus() == PagedVector::PagedVectorStatus::VALID_PV, "Paged Vector must be valid for accessing total number of records.");
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
    PRECONDITION(
        getStatus() == PagedVector::PagedVectorStatus::VALID_PV,
        "Paged Vector must be valid for accessing total number of records in the last page.");
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
    if (getNumberOfPages() > 0)
    {
        return false;
    }
    return true;
}
}
