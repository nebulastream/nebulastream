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
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Nautilus::Interface {

void PagedVectorVarSized::setEntrySizeAndCapacityPerPage() {
    entrySize = 0;

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : this->schema->fields) {
        auto fieldType = field->getDataType();
        if (fieldType->isText()) {
            auto varSizedDataEntryMapKeySize = sizeof(uint64_t);
            entrySize += varSizedDataEntryMapKeySize;
        } else {
            entrySize += physicalDataTypeFactory.getPhysicalType(fieldType)->size();
        }
    }
    capacityPerPage = pageSize / entrySize;
}

PagedVectorVarSized::PagedVectorVarSized(Runtime::BufferManagerPtr bufferManager, SchemaPtr schema, uint64_t pageSize)
    : PagedVectorVarSized(bufferManager, schema, {}, pageSize) {}

PagedVectorVarSized::PagedVectorVarSized(Runtime::BufferManagerPtr bufferManager,
                                         SchemaPtr schema,
                                         std::span<const Runtime::TupleBuffer> buffers,
                                         uint64_t pageSize)
    : bufferManager(std::move(bufferManager)), schema(std::move(schema)), pageSize(pageSize) {
    appendVarSizedDataPage();
    varSizedDataEntryMapCounter = 0;
    totalNumberOfEntries = 0;
    setEntrySizeAndCapacityPerPage();

    // store pages with records
    this->pages.assign(buffers.begin(), buffers.end());

    if (this->pages.empty()) {
        appendPage();
    } else {
        numberOfEntriesOnCurrPage = this->pages.back().getNumberOfTuples();
    }

    for (size_t index = 0; index < this->pages.size(); index++) {
        totalNumberOfEntries += this->pages[index].getNumberOfTuples();
    }
    NES_ERROR("After recreation {}, total {}", numberOfEntriesOnCurrPage, totalNumberOfEntries);
    NES_ASSERT2_FMT(entrySize > 0, "EntrySize for a pagedVector has to be larger than 0!");
    NES_ASSERT2_FMT(capacityPerPage > 0, "At least one tuple has to fit on a page!");
}

void PagedVectorVarSized::appendPage() {
    auto page = bufferManager->getUnpooledBuffer(pageSize);
    if (page.has_value()) {
        if (!pages.empty()) {
            pages.back().setNumberOfTuples(numberOfEntriesOnCurrPage);
        }
        pages.emplace_back(page.value());
        numberOfEntriesOnCurrPage = 0;
    } else {
        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
    }
}

void PagedVectorVarSized::appendVarSizedDataPage() {
    auto page = bufferManager->getUnpooledBuffer(pageSize);
    if (page.has_value()) {
        varSizedDataPages.emplace_back(page.value());
        currVarSizedDataEntry = page.value().getBuffer();
    } else {
        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
    }
}

uint64_t PagedVectorVarSized::storeText(const char* text, uint32_t length) {
    NES_ASSERT2_FMT(length > 0, "Length of text has to be larger than 0!");
    // create a new entry for the varSizedDataEntryMap
    VarSizedDataEntryMapValue textMapValue{currVarSizedDataEntry, length, varSizedDataPages.size() - 1};

    // store the text in the varSizedDataPages
    while (length > 0) {
        auto remainingSpace = pageSize - (currVarSizedDataEntry - varSizedDataPages.back().getBuffer());
        if (remainingSpace >= length) {
            std::memcpy(currVarSizedDataEntry, text, length);
            currVarSizedDataEntry += length;
            break;
        } else {
            std::memcpy(currVarSizedDataEntry, text, remainingSpace);
            text += remainingSpace;
            length -= remainingSpace;
            appendVarSizedDataPage();
        }
    }

    varSizedDataEntryMap.insert(std::make_pair(++varSizedDataEntryMapCounter, textMapValue));
    return varSizedDataEntryMapCounter;
}

TextValue* PagedVectorVarSized::loadText(uint64_t textEntryMapKey) {
    auto textMapValue = varSizedDataEntryMap.at(textEntryMapKey);
    auto textPtr = textMapValue.entryPtr;
    auto textLength = textMapValue.entryLength;
    auto bufferIdx = textMapValue.entryBufIdx;

    // buffer size should be multiple of pageSize for efficiency reasons
    auto bufferSize = ((textLength + pageSize - 1) / pageSize) * pageSize;
    auto buffer = bufferManager->getUnpooledBuffer(bufferSize);

    if (buffer.has_value()) {
        auto textValue = TextValue::create(buffer.value(), textLength);
        auto destPtr = textValue->str();

        // load the text from the varSizedDataPages into the buffer
        while (textLength > 0) {
            auto varSizedDataPage = varSizedDataPages[bufferIdx];
            ++bufferIdx;
            auto remainingSpace = varSizedDataPage.getBufferSize() - (textPtr - varSizedDataPage.getBuffer());
            if (remainingSpace >= textLength) {
                std::memcpy(destPtr, textPtr, textLength);
                break;
            } else {
                std::memcpy(destPtr, textPtr, remainingSpace);
                textPtr = varSizedDataPages[bufferIdx].getBuffer();
                textLength -= remainingSpace;
                destPtr += remainingSpace;
            }
        }
        return textValue;
    } else {
        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
    }
}

void PagedVectorVarSized::appendAllPages(PagedVectorVarSized& other) {
    // TODO optimize appending the maps, see #4639
    NES_ASSERT2_FMT(entrySize == other.entrySize, "Can not combine PagedVector of different entrySize for now!");

    pages.back().setNumberOfTuples(numberOfEntriesOnCurrPage);

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto entryId = 0UL; entryId < other.totalNumberOfEntries; ++entryId) {
        auto pageIdx = entryId / other.capacityPerPage;
        auto entryIdxOnPage = entryId % other.capacityPerPage;
        auto entryPtr = other.pages[pageIdx].getBuffer() + entryIdxOnPage * other.entrySize;

        for (auto& field : schema->fields) {
            auto fieldType = field->getDataType();
            if (fieldType->isText()) {
                auto textEntryMapKey = *reinterpret_cast<uint64_t*>(entryPtr);
                auto textMapValue = other.varSizedDataEntryMap.at(textEntryMapKey);
                textMapValue.entryBufIdx += varSizedDataPages.size();
                varSizedDataEntryMap.insert(std::make_pair(++varSizedDataEntryMapCounter, textMapValue));
                std::memcpy(entryPtr, &varSizedDataEntryMapCounter, sizeof(uint64_t));
                entryPtr += sizeof(uint64_t);
            } else {
                entryPtr += physicalDataTypeFactory.getPhysicalType(fieldType)->size();
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

void PagedVectorVarSized::removeEmptyTrailingVarSizedPages(uint64_t firstInvalidVarSizedPtr) {
    if (varSizedDataEntryMap.find(firstInvalidVarSizedPtr) == varSizedDataEntryMap.end()) {
        NES_ERROR("Key not found {}", firstInvalidVarSizedPtr)
        NES_THROW_RUNTIME_ERROR("Specified key for text does not exist in this PagedVectorVarSized");
    }

    auto lastBufIdx = varSizedDataEntryMap[firstInvalidVarSizedPtr].entryBufIdx;
    auto lastEntry = varSizedDataEntryMap[firstInvalidVarSizedPtr].entryPtr;

    NES_DEBUG("Removing {} of {} total var sized pages (={}), because the last valid string ended on page {} (0-indexed)."
              "Furthermore, there were {} entries on all pages before",
              varSizedDataPages.size() - (lastBufIdx + 1),
              varSizedDataPages.size(),
              lastBufIdx + 1,
              lastBufIdx,
              varSizedDataEntryMap.size())

    // Removing all varSized pages starting from the one after the one where the last entry ends
    varSizedDataPages.erase(varSizedDataPages.begin() + lastBufIdx + 1, varSizedDataPages.end());

    // Delete all invalid entries. std::map is appended in order, so we can simply delete all pointers starting from the first invalid
    varSizedDataEntryMap.erase(varSizedDataEntryMap.find(firstInvalidVarSizedPtr), varSizedDataEntryMap.end());

    NES_DEBUG("New number of var sized data pages is {} and the new amount of entries is {}",
              varSizedDataPages.size(),
              varSizedDataEntryMap.size())

    varSizedDataEntryMapCounter = varSizedDataEntryMap.size();
    currVarSizedDataEntry = lastEntry;
}

void PagedVectorVarSized::removeEmptyTrailingPages(uint64_t numberOfEmptyTrailingPages) {
    NES_DEBUG("Removing {} pages from the pagedVectorVarSized. Number of pages before {}",
              numberOfEmptyTrailingPages,
              pages.size())

    pages.erase(pages.end() - numberOfEmptyTrailingPages, pages.end());

    NES_DEBUG("Number of pages after {}. (If the number of pages equals 0 a new empty page will be added immediately)",
              pages.size())
    if (pages.size() == 0) {
        appendPage();
    }
}

std::vector<Runtime::TupleBuffer>& PagedVectorVarSized::getPages() { return pages; }

uint64_t PagedVectorVarSized::getNumberOfPages() { return pages.size(); }

uint64_t PagedVectorVarSized::getNumberOfVarSizedPages() { return varSizedDataPages.size(); }

uint64_t PagedVectorVarSized::getNumberOfEntries() const { return totalNumberOfEntries; }

uint64_t PagedVectorVarSized::getNumberOfEntriesOnCurrentPage() const { return numberOfEntriesOnCurrPage; }

bool PagedVectorVarSized::varSizedDataEntryMapEmpty() const { return varSizedDataEntryMap.empty(); }

uint64_t PagedVectorVarSized::getVarSizedDataEntryMapCounter() const { return varSizedDataEntryMapCounter; }

uint64_t PagedVectorVarSized::getEntrySize() const { return entrySize; }

uint64_t PagedVectorVarSized::getCapacityPerPage() const { return capacityPerPage; }

}// namespace NES::Nautilus::Interface
