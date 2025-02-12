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
#include <utility>
#include <vector>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/PinnedBuffer.hpp>
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
}

void PagedVector::copyFrom(const PagedVector& other)
{
    pages.insert(pages.end(), other.pages.begin(), other.pages.end());
}

const Memory::PinnedBuffer& PagedVector::getTupleBufferForEntry(const uint64_t entryPos) const
{
    auto bufferPos = entryPos;
    for (const auto& page : pages)
    {
        const auto numTuplesOnPage = page.getNumberOfTuples();
        if (bufferPos < numTuplesOnPage)
        {
            return page;
        }

        bufferPos -= numTuplesOnPage;
    }

    throw CannotAccessBuffer("EntryPos {} exceeds the number of entries in the PagedVector {}!", entryPos, getTotalNumberOfEntries());
}

uint64_t PagedVector::getBufferPosForEntry(const uint64_t entryPos) const
{
    auto bufferPos = entryPos;
    for (const auto& page : pages)
    {
        const auto numTuplesOnPage = page.getNumberOfTuples();
        if (bufferPos < numTuplesOnPage)
        {
            return bufferPos;
        }

        bufferPos -= numTuplesOnPage;
    }

    throw CannotAccessBuffer("EntryPos {} exceeds the number of entries in the PagedVector {}!", entryPos, getTotalNumberOfEntries());
}

uint64_t PagedVector::getTotalNumberOfEntries() const
{
    auto totalNumEntries = 0UL;
    for (const auto& page : pages)
    {
        totalNumEntries += page.getNumberOfTuples();
    }
    return totalNumEntries;
}

const Memory::PinnedBuffer& PagedVector::getLastPage() const
{
    return pages.back();
}

const Memory::PinnedBuffer& PagedVector::getFirstPage() const
{
    return pages.front();
}

uint64_t PagedVector::getNumberOfPages() const
{
    return pages.size();
}
}
