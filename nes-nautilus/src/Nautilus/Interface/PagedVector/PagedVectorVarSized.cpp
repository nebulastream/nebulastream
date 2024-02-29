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

#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>
#include <Util/Logger/Logger.hpp>
#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::Nautilus::Interface {
PagedVectorVarSized::PagedVectorVarSized(Runtime::BufferManagerPtr bufferManager,
                                         NES::SchemaPtr schema,
                                         uint64_t pageSize)
    : bufferManager(std::move(bufferManager)), schema(std::move(schema)), pageSize(pageSize) {
    appendPage();
    appendVarSizedDataPage();
    varSizedDataEntryMapCounter = 0;
    totalNumberOfEntries = 0;
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

void PagedVectorVarSized::appendVarSizedDataPage(){
    auto page = bufferManager->getUnpooledBuffer(pageSize);
    if (page.has_value()) {
        varSizedDataPages.emplace_back(page.value());
        currVarSizedDataEntry = page.value().getBuffer();
    } else {
        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
    }
}

uint64_t PagedVectorVarSized::storeText(const char* text, uint32_t length) {
    // TODO currently the varSizedData must fit onto one single page, see #4638
    NES_ASSERT2_FMT(length > 0, "Length of text has to be larger than 0!");
    NES_ASSERT2_FMT(length <= pageSize, "Length of text has to be smaller than the page size!");

    if (currVarSizedDataEntry + length > varSizedDataPages.back().getBuffer() + pageSize) {
        appendVarSizedDataPage();
    }
    std::memcpy(currVarSizedDataEntry, text, length);

    VarSizedDataEntryMapValue textMapValue;
    textMapValue.entryPtr = currVarSizedDataEntry;
    textMapValue.entryLength = length;
    varSizedDataEntryMap.insert(std::make_pair(++varSizedDataEntryMapCounter, textMapValue));

    currVarSizedDataEntry += length;
    return varSizedDataEntryMapCounter;
}

TextValue* PagedVectorVarSized::loadText(uint64_t textEntryMapKey) {
    auto textMapValue = varSizedDataEntryMap.at(textEntryMapKey);
    auto textPtr = textMapValue.entryPtr;
    auto textLength = textMapValue.entryLength;

    auto buffer = bufferManager->getUnpooledBuffer(PagedVectorVarSized::PAGE_SIZE);
    if (buffer.has_value()) {
        auto textValue = TextValue::create(buffer.value(), textLength);
        std::memcpy(textValue->str(), textPtr, textLength);
        return textValue;
    } else {
        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
    }
}

void PagedVectorVarSized::appendAllPages(PagedVectorVarSized& other) {
    // TODO optimize appending the maps, see #4639
    NES_ASSERT2_FMT(pageSize == other.pageSize, "Can not combine PagedVector of different pageSizes for now!");
    NES_ASSERT2_FMT(entrySize == other.entrySize, "Can not combine PagedVector of different entrySize for now!");

    pages.back().setNumberOfTuples(numberOfEntriesOnCurrPage);
    other.pages.back().setNumberOfTuples(other.numberOfEntriesOnCurrPage);

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

std::vector<Runtime::TupleBuffer>& PagedVectorVarSized::getPages() { return pages; }

uint64_t PagedVectorVarSized::getNumberOfPages() { return pages.size(); }

uint64_t PagedVectorVarSized::getNumberOfVarSizedPages() { return varSizedDataPages.size(); }

uint64_t PagedVectorVarSized::getNumberOfEntries() const { return totalNumberOfEntries; }

uint64_t PagedVectorVarSized::getNumberOfEntriesOnCurrentPage() const { return numberOfEntriesOnCurrPage; }

bool PagedVectorVarSized::varSizedDataEntryMapEmpty() const { return varSizedDataEntryMap.empty(); }

uint64_t PagedVectorVarSized::getVarSizedDataEntryMapCounter() const { return varSizedDataEntryMapCounter; }

uint64_t PagedVectorVarSized::getEntrySize() const { return entrySize; }
PagedVectorVarSized::~PagedVectorVarSized() {
    NES_INFO("PagedVectorVarSized: numPages: {}, numVarSizedPages: {}, numMapEntries: {}", pages.size(), varSizedDataPages.size(), varSizedDataEntryMap.size());
}

} //NES::Nautilus::Interface
