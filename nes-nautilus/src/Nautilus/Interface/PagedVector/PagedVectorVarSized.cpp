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
#include <fstream>

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
    NES_ASSERT2_FMT(length > 0, "Length of text has to be larger than 0!");
    // create a new entry for the varSizedDataEntryMap
    VarSizedDataEntryMapValue textMapValue(currVarSizedDataEntry, length, varSizedDataPages.size() - 1);

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
            auto remainingSpace = pageSize - (textPtr - varSizedDataPages[bufferIdx++].getBuffer());
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
    NES_ASSERT2_FMT(pageSize == other.pageSize, "Can not combine PagedVector of different pageSizes for now!");
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

std::vector<Runtime::TupleBuffer>& PagedVectorVarSized::getPages() { return pages; }

uint64_t PagedVectorVarSized::getNumberOfPages() { return pages.size(); }

uint64_t PagedVectorVarSized::getNumberOfVarSizedPages() { return varSizedDataPages.size(); }

uint64_t PagedVectorVarSized::getNumberOfEntries() const { return totalNumberOfEntries; }

uint64_t PagedVectorVarSized::getNumberOfEntriesOnCurrentPage() const { return numberOfEntriesOnCurrPage; }

bool PagedVectorVarSized::varSizedDataEntryMapEmpty() const { return varSizedDataEntryMap.empty(); }

uint64_t PagedVectorVarSized::getVarSizedDataEntryMapCounter() const { return varSizedDataEntryMapCounter; }

uint64_t PagedVectorVarSized::getEntrySize() const { return entrySize; }

void PagedVectorVarSized::serialize([[maybe_unused]] std::ostream& out) {

    // Save number of records in paged vector
    out.write(reinterpret_cast<char*>(totalNumberOfEntries), sizeof(uint64_t));

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    auto numPages = totalNumberOfEntries / capacityPerPage;

    // 2. Go over pages
    for (auto pageIdx = 0UL; pageIdx <= numPages; ++pageIdx) {
        // link to the start of the current page
        auto entryPtr = pages[pageIdx].getBuffer();
        // calculate global entry index
        auto currGlobalEntryIndx = pageIdx * capacityPerPage;

        // go over entries on the page
        for (auto entryIndx = 0UL; entryIndx < capacityPerPage && currGlobalEntryIndx + entryIndx  < totalNumberOfEntries; ++entryIndx) {

            // go over fields in record
            for (auto& field : schema->fields) {
                auto fieldType = field->getDataType();
                if (fieldType->isText()) {
                    // get text entry id from page
                    auto textEntryMapKey = *reinterpret_cast<uint64_t*>(entryPtr);
                    auto textMapValue = varSizedDataEntryMap.at(textEntryMapKey);
                    auto textValLength = textMapValue.entryLength;

                    // serialize text field of entry: write the size of variable-sized text
                    out.write(reinterpret_cast<char*>(textValLength), sizeof(uint64_t));
                    entryPtr += sizeof(uint64_t);

                    // write variable-sized data
                    //char* dest = new char[textValLength];
                    //std::memcpy(dest, textMapValue.entryPtr, textValLength);
                    out.write(reinterpret_cast<char*>(entryPtr), textValLength);
                    //delete[] dest;

                    entryPtr += textValLength;
                } else {
                    auto fieldSize = physicalDataTypeFactory.getPhysicalType(fieldType)->size();

                    //auto dest = new std::byte[fieldSize];
                    //std::memcpy(dest, entryPtr, fieldSize);
                    out.write(reinterpret_cast<char*>(entryPtr), fieldSize);
                    //delete[] dest;

                    entryPtr += fieldSize;
                }
            }
        }
    }
}

/*
 std::shared_ptr<PagedVectorVarSized> PagedVectorVarSized::deserialize(std::ifstream& in, Runtime::BufferManagerPtr& bufferManager, SchemaPtr& schema, uint64_t pageSize) {
     auto pageVector = std::make_unique<Nautilus::Interface::PagedVectorVarSized>(bufferManager, schema, pageSize);

     // read number of entries in the file
     in.read(reinterpret_cast<char*>(pageVector->totalNumberOfEntries), sizeof(uint64_t));

     DefaultPhysicalTypeFactory physicalDataTypeFactory;
     auto numPages = pageVector->totalNumberOfEntries / pageVector->capacityPerPage;

     for (auto pageIdx = 0UL; pageIdx <= numPages; ++pageIdx) {
         auto entryPtr = pageVector->pages[pageIdx].getBuffer();
         auto currGlobalEntryIndx = pageIdx * pageVector->capacityPerPage;

         for (auto entryIndx = 0UL; entryIndx < pageVector->capacityPerPage && currGlobalEntryIndx + entryIndx  < pageVector->totalNumberOfEntries; ++entryIndx) {
             for (auto& field : pageVector->schema->fields) {
                 auto fieldType = field->getDataType();
                 if (fieldType->isText()) {

                     // read size of the text value
                     uint64_t textSize;
                     in.read(reinterpret_cast<char*>(&textSize), sizeof(uint64_t));
                     entryPtr += sizeof(uint64_t);

                     VarSizedDataEntryMapValue textMapValue(pageVector->currVarSizedDataEntry, textSize, pageVector->varSizedDataPages.size() - 1);

                     auto curLength = textSize;
                     while (textSize > 0) {
                         auto remainingSpace = pageVector->pageSize - (pageVector->currVarSizedDataEntry - pageVector->varSizedDataPages.back().getBuffer());
                         if (remainingSpace >= curLength) {
                             in.read(reinterpret_cast<char*>(pageVector->currVarSizedDataEntry), curLength);
                             pageVector->currVarSizedDataEntry += curLength;
                             entryPtr += textSize;
                             break;
                         } else {
                             in.read(reinterpret_cast<char*>(pageVector->currVarSizedDataEntry), remainingSpace);
                             entryPtr += remainingSpace;
                             curLength -= remainingSpace;
                             pageVector->appendVarSizedDataPage();
                         }
                     }

                     pageVector->varSizedDataEntryMap.insert(std::make_pair(++pageVector->varSizedDataEntryMapCounter, textMapValue));
                 } else {
                     auto fieldSize = physicalDataTypeFactory.getPhysicalType(fieldType)->size();
                     in.read(reinterpret_cast<char*>(entryPtr), fieldSize);
                     entryPtr += fieldSize;
                 }
             }
         }
         pageVector->appendPage();
     }
    return pageVector;
}*/

void PagedVectorVarSized::deserialize(std::ifstream& in) {
     // read number of entries in the file
     in.read(reinterpret_cast<char*>(totalNumberOfEntries), sizeof(uint64_t));

     DefaultPhysicalTypeFactory physicalDataTypeFactory;
     auto numPages = totalNumberOfEntries / capacityPerPage;

     for (auto pageIdx = 0UL; pageIdx <= numPages; ++pageIdx) {
         auto entryPtr = pages[pageIdx].getBuffer();
         auto currGlobalEntryIndx = pageIdx * capacityPerPage;

         for (auto entryIndx = 0UL; entryIndx < capacityPerPage && currGlobalEntryIndx + entryIndx  < totalNumberOfEntries; ++entryIndx) {
             for (auto& field : schema->fields) {
                 auto fieldType = field->getDataType();
                 if (fieldType->isText()) {

                     // read size of the text value
                     uint64_t textSize;
                     in.read(reinterpret_cast<char*>(&textSize), sizeof(uint64_t));
                     entryPtr += sizeof(uint64_t);

                     VarSizedDataEntryMapValue textMapValue(currVarSizedDataEntry, textSize, varSizedDataPages.size() - 1);

                     auto curLength = textSize;
                     while (textSize > 0) {
                         auto remainingSpace = pageSize - (currVarSizedDataEntry - varSizedDataPages.back().getBuffer());
                         if (remainingSpace >= curLength) {
                             in.read(reinterpret_cast<char*>(currVarSizedDataEntry), curLength);
                             currVarSizedDataEntry += curLength;
                             entryPtr += textSize;
                             break;
                         } else {
                             in.read(reinterpret_cast<char*>(currVarSizedDataEntry), remainingSpace);
                             entryPtr += remainingSpace;
                             curLength -= remainingSpace;
                             appendVarSizedDataPage();
                         }
                     }

                     varSizedDataEntryMap.insert(std::make_pair(++varSizedDataEntryMapCounter, textMapValue));
                 } else {
                     auto fieldSize = physicalDataTypeFactory.getPhysicalType(fieldType)->size();
                     in.read(reinterpret_cast<char*>(entryPtr), fieldSize);
                     entryPtr += fieldSize;
                 }
             }
         }
         appendPage();
     }
}

} //NES::Nautilus::Interface
