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
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include <fmt/ranges.h>
#include <Util/Logger/Logger.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <bits/ranges_algo.h>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::Interface
{

void PagedVector::appendPageIfFull(Memory::AbstractBufferProvider* bufferProvider, const Memory::MemoryLayouts::MemoryLayout* memoryLayout)
{
    PRECONDITION(bufferProvider != nullptr, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(memoryLayout != nullptr, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(memoryLayout->getTupleSize() > 0, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(memoryLayout->getCapacity() > 0, "At least one tuple has to fit on a page!");

    if (pages.empty() || pages.back().getNumberOfTuples() >= memoryLayout->getCapacity())
    {
        if (auto page = bufferProvider->getUnpooledBuffer(memoryLayout->getBufferSize()); page.has_value())
        {
            const auto currentAccumulatedNumberOfEntries = accumulatedNumberOfEntries.empty() ? 0 : accumulatedNumberOfEntries.back();
            accumulatedNumberOfEntries.emplace_back(currentAccumulatedNumberOfEntries);
            pages.emplace_back(page.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available!");
        }
    }
}

void PagedVector::appendAllPages(PagedVector& other)
{
    copyFrom(other);
    other.pages.clear();
    other.accumulatedNumberOfEntries.clear();
}

void PagedVector::copyFrom(const PagedVector& other)
{
    pages.insert(pages.end(), other.pages.begin(), other.pages.end());

    /// We can not simply combine the accumulatedNumberOfEntries vectors, as the entries are a accumulated sum of the number of entries.
    /// Therefore, we need to iterate over each entry and add the number of entries of the previous entry.
    auto previousAccumulatedNumberOfEntries = accumulatedNumberOfEntries.empty() ? 0 : accumulatedNumberOfEntries.back();
    for (const auto& numEntries : other.accumulatedNumberOfEntries)
    {
        accumulatedNumberOfEntries.emplace_back(previousAccumulatedNumberOfEntries + numEntries);
    }
}

void PagedVector::increaseAccumulatedNumberOfEntries()
{
    const auto currentNoOfEntries = accumulatedNumberOfEntries.back();
    accumulatedNumberOfEntries.back() = currentNoOfEntries + 1;
}

const Memory::TupleBuffer& PagedVector::getTupleBufferForEntry(const uint64_t entryPos)
{
    // TBA
    const auto entryIndex = findPageIndex(entryPos);
    NES_TRACE("EntryPos {} ---> entryIndex {} with the following accumulatedNumberOfEntries: {}", entryPos, entryIndex, fmt::join(accumulatedNumberOfEntries, ","));
    return pages[entryIndex];
}

uint64_t PagedVector::findPageIndex(const uint64_t& pos)
{
    ++counter;

    /// If the entry pos is larger than the total number of entries, we will not find the position in a tuplebuffer for it
    if (pos >= getTotalNumberOfEntries())
    {
        throw CannotAccessBuffer("EntryPos {} exceeds the number of entries in the PagedVector {}! With the following accumulatedNumberOfEntries: {}", pos, getTotalNumberOfEntries(), fmt::join(accumulatedNumberOfEntries, ","));
    }

    // TBA
    const auto it = std::ranges::upper_bound(accumulatedNumberOfEntries, pos);
    return (it == accumulatedNumberOfEntries.begin()) ? 0 : (it - accumulatedNumberOfEntries.begin());
}

uint64_t PagedVector::getBufferPosForEntry(const uint64_t entryPos)
{
    // TBA
    const auto entryIndex = findPageIndex(entryPos);
    const auto entryOffset = entryPos - (entryIndex == 0 ? 0 : accumulatedNumberOfEntries[entryIndex - 1]);
    NES_TRACE("EntryPos {} ---> entryIndex {} and entryOffset {} with the following accumulatedNumberOfEntries: {}", entryPos, entryIndex, entryOffset, fmt::join(accumulatedNumberOfEntries, ","));
    return entryOffset;
}

uint64_t PagedVector::getTotalNumberOfEntries() const
{
    return (accumulatedNumberOfEntries.empty() ? 0 : accumulatedNumberOfEntries.back());
}

const Memory::TupleBuffer& PagedVector::getLastPage() const
{
    return pages.back();
}

const Memory::TupleBuffer& PagedVector::getFirstPage() const
{
    return pages.front();
}

uint64_t PagedVector::getNumberOfPages() const
{
    return pages.size();
}
}
