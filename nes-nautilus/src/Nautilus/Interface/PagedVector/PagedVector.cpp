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
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::Interface
{

void PagedVector::appendPageIfFull(Memory::AbstractBufferProvider* bufferProvider, const Memory::MemoryLayouts::MemoryLayout* memoryLayout)
{
    PRECONDITION(bufferProvider != nullptr, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(memoryLayout != nullptr, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(memoryLayout->getTupleSize() > 0, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(memoryLayout->getCapacity() > 0, "At least one tuple has to fit on a page!");

    if (pages.getNumberOfPages() == 0 || pages.getNumberOfTuplesLastPage() >= memoryLayout->getCapacity())
    {
        if (const auto page = bufferProvider->getUnpooledBuffer(memoryLayout->getBufferSize()); page.has_value())
        {
            pages.addPage(page.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available!");
        }
    }
}

void PagedVector::PagesWrapper::updateCumulativeSumLastItem()
{
    if (pages.empty())
    {
        return;
    }
    const auto penultimateCumulativeSum = (pages.size() >= 2) ? pages.rbegin()[1].cumulativeSum : 0;
    auto& lastItem = pages.back();
    lastItem.cumulativeSum = lastItem.buffer.getNumberOfTuples() + penultimateCumulativeSum;
}

void PagedVector::PagesWrapper::updateCumulativeSumAllPages()
{
    size_t curCumulativeSum = 0;
    for (auto& page : pages)
    {
        page.cumulativeSum = page.buffer.getNumberOfTuples() + curCumulativeSum;
        curCumulativeSum = page.cumulativeSum;
    }
}

void PagedVector::moveAllPages(PagedVector& other)
{
    copyFrom(other);
    other.pages.clearPages();
}

void PagedVector::copyFrom(const PagedVector& other)
{
    pages.addPages(other.pages);
}


const Memory::TupleBuffer* PagedVector::getTupleBufferForEntry(const uint64_t entryPos) const
{
    /// We need to find the index / page that the entryPos belongs to.
    /// If an index exists for this, we get the tuple buffer
    if (const auto index = pages.findIdx(entryPos); index.has_value())
    {
        const auto indexVal = index.value();
        return std::addressof(pages[indexVal].buffer);
    }
    return nullptr;
}

std::optional<uint64_t> PagedVector::getBufferPosForEntry(const uint64_t entryPos) const
{
    /// We need to find the index / page that the entryPos belongs to.
    return pages.findIdx(entryPos).and_then(
        [&](const size_t index) -> std::optional<uint64_t>
        {
            /// We need to subtract the cumulative sum before our found index to get the position on the page
            const auto cumulativeSumBefore = (index == 0) ? 0 : pages[index - 1].cumulativeSum;
            return entryPos - cumulativeSumBefore;
        });
}

uint64_t PagedVector::PagesWrapper::getNumberOfTuplesLastPage() const
{
    return getLastPage().getNumberOfTuples();
}

std::vector<PagedVector::TupleBufferWithCumulativeSum>::iterator PagedVector::PagesWrapper::begin()
{
    return pages.begin();
}

std::vector<PagedVector::TupleBufferWithCumulativeSum>::iterator PagedVector::PagesWrapper::end()
{
    return pages.end();
}

const PagedVector::TupleBufferWithCumulativeSum& PagedVector::PagesWrapper::operator[](const size_t index) const
{
    return pages.at(index);
}

void PagedVector::PagesWrapper::addPage(const Memory::TupleBuffer& newPage)
{
    updateCumulativeSumLastItem();
    pages.emplace_back(newPage);
}

void PagedVector::PagesWrapper::addPages(const PagesWrapper& other)
{
    pages.insert(pages.end(), other.pages.begin(), other.pages.end());
    updateCumulativeSumAllPages();
}

void PagedVector::PagesWrapper::clearPages()
{
    pages.clear();
}

std::optional<size_t> PagedVector::PagesWrapper::findIdx(const uint64_t entryPos) const
{
    if (entryPos >= getTotalNumberOfEntries())
    {
        NES_WARNING("EntryPos {} exceeds the number of entries in the PagedVector {}!", entryPos, getTotalNumberOfEntries());
        return {};
    }

    /// Use std::lower_bound to find the first cumulative sum greater than entryPos
    auto projection = [&](const TupleBufferWithCumulativeSum& bufferWithSum) -> size_t
    {
        /// The -1 is important as we need to subtract one due to starting the entryPos at 0.
        /// Otherwise, {4, 12, 14} and entryPos 12 would return the iterator to 12 and not to 14
        /// Also, as the cumulative sum on the last page might not have been updated since the
        /// last write operation, we need to use the number of tuples in the buffer instead
        if (&bufferWithSum == &pages.back())
        {
            const auto penultimateCumulativeSum = (pages.size() > 1) ? std::prev(pages.end(), 2)->cumulativeSum : 0;
            return penultimateCumulativeSum + bufferWithSum.buffer.getNumberOfTuples() - 1;
        }
        return bufferWithSum.cumulativeSum - 1;
    };
    const auto it = std::ranges::lower_bound(pages, entryPos, std::less<>{}, projection);

    if (it != pages.end())
    {
        return std::distance(pages.begin(), it);
    }
    return {};
}

uint64_t PagedVector::PagesWrapper::getTotalNumberOfEntries() const
{
    /// We can not ensure that the last cumulative sum is up-to-date. Therefore, we need to add the penultimate sum + no. tuples of last page
    const auto penultimateCumulativeSum = (pages.size() > 1) ? pages.rbegin()[1].cumulativeSum : 0;
    const auto lastNumberOfTuples = (not pages.empty()) ? pages.rbegin()[0].buffer.getNumberOfTuples() : 0;
    return penultimateCumulativeSum + lastNumberOfTuples;
}

const Memory::TupleBuffer& PagedVector::PagesWrapper::getLastPage() const
{
    PRECONDITION(not pages.empty(), "getLastPage() should be called after a page has been inserted!");
    return pages.back().buffer;
}

const Memory::TupleBuffer& PagedVector::PagesWrapper::getFirstPage() const
{
    PRECONDITION(not pages.empty(), "getFirstPage() should be called after a page has been inserted!");
    return pages.front().buffer;
}

uint64_t PagedVector::PagesWrapper::getNumberOfPages() const
{
    return pages.size();
}
}
