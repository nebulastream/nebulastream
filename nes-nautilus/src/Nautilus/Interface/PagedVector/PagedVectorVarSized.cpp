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
    totalNumberOfEntries = 0;
    entrySize = 0;
    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto fieldType = field->getDataType();
        if (fieldType->isText()) {
            auto varSizedDataPtrSize = sizeof(int8_t*);
            auto varSizedDataSize = sizeof(uint32_t);
            entrySize += varSizedDataPtrSize + varSizedDataSize;
        } else {
            entrySize += physicalDataTypeFactory.getPhysicalType(fieldType)->size();
        }
    }
    capacityPerPage = entrySize / pageSize;
    NES_ASSERT2_FMT(entrySize > 0, "EntrySize for a pagedVector has to be larger than 0!");
    NES_ASSERT2_FMT((entrySize / pageSize) > 0, "At least one tuple has to fit on a page!");
}

void PagedVectorVarSized::appendPage() {
    auto page = bufferManager->getUnpooledBuffer(pageSize);
    if (page.has_value()) {
        pages.emplace_back(page.value());
        numberOfEntriesOnCurrPage = 0;
    } else {
        NES_THROW_RUNTIME_ERROR("Couldn't get unpooled buffer!");
    }
}

void PagedVectorVarSized::appendVarSizedDataPage(){
    auto page = bufferManager->getUnpooledBuffer(pageSize);
    if (page.has_value()) {
        varSizedDataPages.emplace_back(page.value());
        currVarSizedDataEntry = page.value().getBuffer();
    } else {
        NES_THROW_RUNTIME_ERROR("Couldn't get unpooled buffer!");
    }
}

void PagedVectorVarSized::storeText(const char* text, uint64_t length) {
    NES_ASSERT2_FMT(length > 0, "Length of text has to be larger than 0!");
    NES_ASSERT2_FMT(length <= pageSize, "Length of text has to be smaller than the page size!");

    if (currVarSizedDataEntry + length > varSizedDataPages.back().getBuffer() + pageSize) {
        appendVarSizedDataPage();
    }
    std::memcpy(currVarSizedDataEntry, text, length);
    currVarSizedDataEntry += length;
}

std::string PagedVectorVarSized::loadText(uint8_t* textPtr, uint32_t length) {
    NES_ASSERT2_FMT(length > 0, "Length of text has to be larger than 0!");
    NES_ASSERT2_FMT(length <= pageSize, "Length of text has to be smaller than the page size!");

    std::string result;
    result.append(reinterpret_cast<char*>(textPtr), length);
    return result;
}

std::vector<Runtime::TupleBuffer>& PagedVectorVarSized::getPages() { return pages; }

uint64_t PagedVectorVarSized::getNumberOfPages() { return pages.size(); }

} //NES::Nautilus::Interface
