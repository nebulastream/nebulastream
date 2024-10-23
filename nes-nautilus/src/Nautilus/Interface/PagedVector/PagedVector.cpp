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

PagedVector::~PagedVector()
{
    for (auto& page : pages)
    {
        delete page;
    }
}

Memory::TupleBuffer* PagedVector::appendPage()
{
    auto page = bufferProvider->getUnpooledBuffer(memoryLayout->getBufferSize());
    if (page.has_value())
    {
        pages.emplace_back(new Memory::TupleBuffer(page.value()));
        return reinterpret_cast<Memory::TupleBuffer *>(pages.data());
    }

    NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
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
    for (const auto* page : pages)
    {
        totalNumEntries += page->getNumberOfTuples();
    }
    return totalNumEntries;
}

std::vector<Memory::TupleBuffer*>& PagedVector::getPages()
{
    return pages;
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
