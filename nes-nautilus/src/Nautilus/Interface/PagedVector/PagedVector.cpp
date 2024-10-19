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
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Interface
{

PagedVector::PagedVector(
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider, Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout)
    : bufferProvider(std::move(bufferProvider)), memoryLayout(std::move(memoryLayout))
{
    NES_ASSERT2_FMT(this->memoryLayout->getTupleSize() > 0, "EntrySize for a pagedVector has to be larger than 0!");
    NES_ASSERT2_FMT(this->memoryLayout->getCapacity() > 0, "At least one tuple has to fit on a page!");

    appendPage();
}

void PagedVector::appendPage()
{
    auto page = bufferProvider->getUnpooledBuffer(memoryLayout->getBufferSize());
    if (page.has_value())
    {
        pages.emplace_back(page.value());
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
    }
}

Memory::TupleBuffer* PagedVector::getTupleBufferForEntry(uint64_t entryPos)
{
    getTupleBufferAndPosForEntry(entryPos);
    return tupleBufferAndPosForEntry.buffer;
}

uint64_t PagedVector::getBufferPosForEntry(uint64_t entryPos)
{
    getTupleBufferAndPosForEntry(entryPos);
    return tupleBufferAndPosForEntry.bufferPos;
}

void PagedVector::appendAllPages(PagedVector& other)
{
    NES_ASSERT2_FMT(
        memoryLayout->getSchema()->hasEqualTypes(other.memoryLayout->getSchema()),
        "Cannot combine PagedVectors with different PhysicalTypes!");

    pages.insert(pages.end(), other.pages.begin(), other.pages.end());
    other.pages.clear();
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

void PagedVector::getTupleBufferAndPosForEntry(uint64_t entryPos)
{
    if (entryPos == tupleBufferAndPosForEntry.entryPos)
    {
        return;
    }

    tupleBufferAndPosForEntry.entryPos = entryPos;
    for (auto& page : pages)
    {
        auto numTuplesOnPage = page.getNumberOfTuples();
        if (entryPos < numTuplesOnPage)
        {
            tupleBufferAndPosForEntry.bufferPos = entryPos;
            tupleBufferAndPosForEntry.buffer = new Memory::TupleBuffer(page);
            return;
        }

        entryPos -= numTuplesOnPage;
    }

    tupleBufferAndPosForEntry.bufferPos = 0;
    tupleBufferAndPosForEntry.buffer = nullptr;
}

std::vector<Memory::TupleBuffer>& PagedVector::getPages()
{
    return pages;
}

uint64_t PagedVector::getNumberOfPages() const
{
    return pages.size();
}

uint64_t PagedVector::getEntrySize() const
{
    return memoryLayout->getTupleSize();
}

uint64_t PagedVector::getCapacityPerPage() const
{
    return memoryLayout->getCapacity();
}

}
