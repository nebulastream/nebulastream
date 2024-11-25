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
#include <vector>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::Interface
{
PagedVector::PagedVector(
    const std::shared_ptr<Memory::AbstractBufferProvider>& bufferProvider, const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout)
    : bufferProvider(std::move(bufferProvider)), memoryLayout(std::move(memoryLayout))
{
    INVARIANT(this->memoryLayout->getTupleSize() > 0, "EntrySize for a pagedVector has to be larger than 0!");
    INVARIANT(this->memoryLayout->getCapacity() > 0, "At least one tuple has to fit on a page!");

    appendPageIfFull();
}

void PagedVector::appendPageIfFull()
{
    INVARIANT(bufferProvider->getBufferSize() == memoryLayout->getBufferSize());

    if (pages.empty() || pages.back().getNumberOfTuples() >= memoryLayout->getCapacity())
    {
        auto page = bufferProvider->getBufferBlocking();
        pages.emplace_back(page);
    }
}

void PagedVector::appendAllPages(PagedVector& other)
{
    PRECONDITION(
        memoryLayout->getSchema()->hasEqualTypes(other.memoryLayout->getSchema()),
        "Cannot combine PagedVectors with different PhysicalTypes!");

    pages.insert(pages.end(), other.pages.begin(), other.pages.end());
    other.pages.clear();
}

const Memory::TupleBuffer& PagedVector::getTupleBufferForEntry(const uint64_t entryPos) const
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

    throw BufferAccessException("EntryPos {} exceeds the number of entries in the PagedVector {}!", entryPos, getTotalNumberOfEntries());
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

    throw BufferAccessException("EntryPos {} exceeds the number of entries in the PagedVector {}!", entryPos, getTotalNumberOfEntries());
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
