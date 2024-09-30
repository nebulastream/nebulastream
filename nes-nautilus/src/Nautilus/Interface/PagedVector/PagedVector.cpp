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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Interface
{

PagedVector::PagedVector(
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider, Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout)
    : bufferProvider(std::move(bufferProvider))
    , memoryLayout(std::move(memoryLayout))
    , totalNumberOfEntries(0)
    , lastPageRead(nullptr)
    , varSizedDataEntryMapCounter(0)
{
    /// set numberOfEntriesOnCurrPage and currVarSizedDataEntry by appending new pages
    appendVarSizedDataPage();
    appendPage();

    NES_ASSERT2_FMT(this->memoryLayout->getTupleSize() > 0, "EntrySize for a pagedVector has to be larger than 0!");
    NES_ASSERT2_FMT(this->memoryLayout->getCapacity() > 0, "At least one tuple has to fit on a page!");
}

void PagedVector::appendPage()
{
    auto page = bufferProvider->getUnpooledBuffer(memoryLayout->getBufferSize());
    if (page.has_value())
    {
        if (!pages.empty())
        {
            pages.back().setNumberOfTuples(numberOfEntriesOnCurrPage);
        }
        pages.emplace_back(page.value());
        numberOfEntriesOnCurrPage = 0;
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
    }
}

void PagedVector::appendVarSizedDataPage()
{
    auto page = bufferProvider->getUnpooledBuffer(memoryLayout->getBufferSize());
    if (page.has_value())
    {
        varSizedDataPages.emplace_back(page.value());
        currVarSizedDataEntry = page.value().getBuffer();
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
    }
}

uint64_t PagedVector::storeText(const int8_t* text, uint32_t length)
{
    NES_ASSERT2_FMT(length > 0, "Length of text has to be larger than 0!");
    /// create a new entry for the varSizedDataEntryMap to be able to load the text later
    VarSizedDataEntryMapValue textMapValue(currVarSizedDataEntry, length, varSizedDataPages.size() - 1);

    /// store the text in the varSizedDataPages in chunks and update the currVarSizedDataEntry
    while (length > 0)
    {
        /// remaining space in the current varSizedDataPage is the total size of the buffer minus the current position in the buffer
        auto remainingSpace = memoryLayout->getBufferSize() - (currVarSizedDataEntry - varSizedDataPages.back().getBuffer());
        if (remainingSpace >= length)
        {
            std::memcpy(currVarSizedDataEntry, text, length);
            currVarSizedDataEntry += length;
            break;
        }

        std::memcpy(currVarSizedDataEntry, text, remainingSpace);
        text += remainingSpace;
        length -= remainingSpace;
        appendVarSizedDataPage();
    }

    varSizedDataEntryMap.insert(std::make_pair(++varSizedDataEntryMapCounter, textMapValue));
    return varSizedDataEntryMapCounter;
}

int8_t* PagedVector::loadText(uint64_t textEntryMapKey)
{
    NES_ASSERT2_FMT(textEntryMapKey <= varSizedDataEntryMapCounter && textEntryMapKey > 0, "TextEntryMapKey is not valid!");
    /// get the pointer to the text, its length and the buffer index from the varSizedDataEntryMap to calculate the remaining
    /// text size in the buffer and to be able to continue loading from the adjacent buffer
    auto [textPtr, textLength, bufferIdx] = varSizedDataEntryMap.at(textEntryMapKey);

    /// buffer size should be multiple of pageSize for efficiency reasons
    const auto pageSize = memoryLayout->getBufferSize();
    auto bufferSize = ((textLength + pageSize - 1) / pageSize) * pageSize;
    auto buffer = bufferProvider->getUnpooledBuffer(bufferSize);

    if (buffer.has_value())
    {
        auto* textValue = buffer.value().getBuffer<int8_t>();
        auto* destPtr = textValue;

        /// load the text by iterating over the varSizedDataPages and copying the text to the buffer in chunks
        while (textLength > 0)
        {
            auto varSizedDataPage = varSizedDataPages[bufferIdx];
            ++bufferIdx;

            /// remaining space in the target varSizedDataPage is the total size of the buffer minus the text's position in the buffer
            auto remainingSpace = varSizedDataPage.getBufferSize() - (textPtr - varSizedDataPage.getBuffer());
            if (remainingSpace >= textLength)
            {
                std::memcpy(destPtr, textPtr, textLength);
                break;
            }

            std::memcpy(destPtr, textPtr, remainingSpace);
            textPtr = varSizedDataPages[bufferIdx].getBuffer();
            textLength -= remainingSpace;
            destPtr += remainingSpace;
        }

        return textValue;
    }

    NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
}

void PagedVector::appendAllPages(PagedVector& other)
{
    /// TODO optimize appending the maps
    /// one idea is to get the field indices of the text fields beforehand if there is more than one text field as sequential reading is more efficient
    const auto& schema = memoryLayout->getSchema();
    NES_ASSERT2_FMT(schema->hasEqualTypes(other.memoryLayout->getSchema()), "Cannot combine PagedVectors with different PhysicalTypes!");

    pages.back().setNumberOfTuples(numberOfEntriesOnCurrPage);
    const auto otherCapacityPerPage = other.memoryLayout->getCapacity();

    /// update the buffer index of each text in the varSizedDataPages and add the new entries to the varSizedDataEntryMap
    for (auto idx = 0UL; idx < schema->getSize(); ++idx)
    {
        if (schema->fields[idx]->getDataType()->isText())
        {
            for (auto entryId = 0UL; entryId < other.totalNumberOfEntries; ++entryId)
            {
                auto pageIdx = entryId / otherCapacityPerPage;
                auto entryIdxOnPage = entryId % otherCapacityPerPage;
                auto* entryPtr = other.pages[pageIdx].getBuffer() + other.memoryLayout->getFieldOffset(entryIdxOnPage, idx);

                auto textEntryMapKey = *reinterpret_cast<uint64_t*>(entryPtr);
                auto textMapValue = other.varSizedDataEntryMap.at(textEntryMapKey);

                /// buffer index is updated to the new index as the varSizedDataPages are appended to the back while
                /// the pointer to the text and the length remain the same
                textMapValue.entryBufIdx += varSizedDataPages.size();
                varSizedDataEntryMap.insert(std::make_pair(++varSizedDataEntryMapCounter, textMapValue));
                std::memcpy(entryPtr, &varSizedDataEntryMapCounter, sizeof(uint64_t));
            }
        }
    }

    pages.insert(pages.end(), other.pages.begin(), other.pages.end());
    varSizedDataPages.insert(varSizedDataPages.end(), other.varSizedDataPages.begin(), other.varSizedDataPages.end());
    totalNumberOfEntries += other.totalNumberOfEntries;
    numberOfEntriesOnCurrPage = other.numberOfEntriesOnCurrPage;
    currVarSizedDataEntry = other.currVarSizedDataEntry;

    other.pages.clear();
    other.varSizedDataPages.clear();
    other.totalNumberOfEntries = 0;
    other.numberOfEntriesOnCurrPage = 0;
    other.currVarSizedDataEntry = nullptr;
    other.varSizedDataEntryMap.clear();
    other.varSizedDataEntryMapCounter = 0;
}

std::vector<Memory::TupleBuffer>& PagedVector::getPages()
{
    return pages;
}

uint64_t PagedVector::getNumberOfPages() const
{
    return pages.size();
}

uint64_t PagedVector::getNumberOfVarSizedPages() const
{
    return varSizedDataPages.size();
}

uint64_t PagedVector::getNumberOfEntries() const
{
    return totalNumberOfEntries;
}

uint64_t PagedVector::getNumberOfEntriesOnCurrentPage() const
{
    return numberOfEntriesOnCurrPage;
}

bool PagedVector::varSizedDataEntryMapEmpty() const
{
    return varSizedDataEntryMap.empty();
}

uint64_t PagedVector::getVarSizedDataEntryMapCounter() const
{
    return varSizedDataEntryMapCounter;
}

uint64_t PagedVector::getEntrySize() const
{
    return memoryLayout->getTupleSize();
}

uint64_t PagedVector::getCapacityPerPage() const
{
    return memoryLayout->getCapacity();
}

void PagedVector::setLastPageRead(uint8_t* page)
{
    lastPageRead = page;
}

} /// namespace NES::Nautilus::Interface
