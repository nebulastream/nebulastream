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

#include <cstdint>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
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

    if (pages.empty() || pages.back().buffer.getNumberOfTuples() >= memoryLayout->getCapacity())
    {
        if (auto page = bufferProvider->getUnpooledBuffer(memoryLayout->getBufferSize()); page.has_value())
        {
            updateCumulativeSumLastItem();
            // std::memset(page.value().getBuffer(), 0, page.value().getBufferSize());
            pages.emplace_back(page.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available!");
        }
    }
}

void PagedVector::updateCumulativeSumLastItem()
{
    if (pages.empty())
    {
        return;
    }
    const auto penultimateCumulativeSum = (pages.size() >= 2) ? pages.rbegin()[1].cumulativeSum : 0;
    auto& lastItem = pages.back();
    lastItem.cumulativeSum = lastItem.buffer.getNumberOfTuples() + penultimateCumulativeSum;
}

void PagedVector::updateCumulativeSumAllPages()
{
    auto curCumulativeSum = 0;
    for (auto& page : pages)
    {
        page.cumulativeSum = page.buffer.getNumberOfTuples() + curCumulativeSum;
        curCumulativeSum = page.cumulativeSum;
    }
}


void PagedVector::appendAllPages(PagedVector& other)
{
    copyFrom(other);
    other.pages.clear();
}

void PagedVector::copyFrom(const PagedVector& other)
{
    pages.insert(pages.end(), other.pages.begin(), other.pages.end());
    updateCumulativeSumAllPages();
}

std::optional<size_t> PagedVector::findIdx(const uint64_t entryPos) const
{
    if (entryPos >= getTotalNumberOfEntries())
    {
        NES_WARNING("EntryPos {} exceeds the number of entries in the PagedVector {}!", entryPos, getTotalNumberOfEntries());
        return {};
    }

    /// Use std::lower_bound to find the first cumulative sum greater than entryPos
    auto it = std::lower_bound(
        pages.begin(),
        pages.end(),
        entryPos,
        [](const TupleBufferWithCumulativeSum& bufferWithSum, const size_t value)
        {
            /// The -1 is important as we need to subtract one due to starting the entryPos at 0.
            /// Otherwise, {4, 12, 14} and entryPos 12 would return the iterator to 12 and not to 14
            return bufferWithSum.cumulativeSum - 1 < value;
        });

    const auto index = std::distance(pages.begin(), it);
    return index;
}

const Memory::TupleBuffer* PagedVector::getTupleBufferForEntry(const uint64_t entryPos) const
{
    /// We need to find the index / page that the entryPos belongs to.
    /// If an index exists for this, we get the tuple buffer
    const auto index = findIdx(entryPos);
    if (index.has_value())
    {
        const auto indexVal = index.value();
        return std::addressof(pages.at(indexVal).buffer);
    }
    return nullptr;
}

std::optional<uint64_t> PagedVector::getBufferPosForEntry(const uint64_t entryPos) const
{
    /// We need to find the index / page that the entryPos belongs to.
    return findIdx(entryPos).and_then([&](const size_t index) -> std::optional<uint64_t>
    {
        /// We need to subtract the cumulative sum before our found index to get the position on the page
        const auto cumulativeSumBefore = (index == 0) ? 0 : pages.at(index - 1).cumulativeSum;
        return entryPos - cumulativeSumBefore;
    });
}

uint64_t PagedVector::getTotalNumberOfEntries() const
{
    /// We can not ensure that the last cumulative sum is up-to-date. Therefore, we need to add the penultimate sum + no. tuples of last page
    const auto penultimateCumulativeSum = (pages.size() > 1) ? pages.rbegin()[1].cumulativeSum : 0;
    const auto lastNumberOfTuples = (pages.size() > 0) ? pages.rbegin()[0].buffer.getNumberOfTuples() : 0;
    return penultimateCumulativeSum + lastNumberOfTuples;
}

const Memory::TupleBuffer& PagedVector::getLastPage() const
{
    return pages.back().buffer;
}

const Memory::TupleBuffer& PagedVector::getFirstPage() const
{
    return pages.front().buffer;
}

uint64_t PagedVector::getNumberOfPages() const
{
    return pages.size();
}
}
