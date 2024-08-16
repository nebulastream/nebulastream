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
#include <Nautilus/DataTypes/FixedSizeExecutableDataType.hpp>
#include <Nautilus/DataTypes/VariableSizeExecutableDataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <utility>

namespace NES::Nautilus::Interface {
PagedVectorVarSizedRef::PagedVectorVarSizedRef(const MemRefVal& pagedVectorVarSizedRef, SchemaPtr schema)
    : pagedVectorVarSizedRef(pagedVectorVarSizedRef), schema(std::move(schema)) {}

void allocateNewPageVarSizedProxy(void* pagedVectorVarSizedPtr) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    pagedVectorVarSized->appendPage();
}

int8_t* getEntryVarSizedProxy(void* pagedVectorVarSizedPtr, uint64_t entryPos) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    auto& allPages = pagedVectorVarSized->getPages();
    for (auto& page : allPages) {
        auto numTuplesOnPage = page.getNumberOfTuples();

        if (entryPos < numTuplesOnPage) {
            auto entryPtrOnPage = entryPos * pagedVectorVarSized->getEntrySize();
            return page.getBuffer<int8_t>() + entryPtrOnPage;
        } else {
            entryPos -= numTuplesOnPage;
        }
    }

    // As we might have not set the number of tuples of the last tuple buffer
    if (entryPos < pagedVectorVarSized->getNumberOfEntriesOnCurrentPage()) {
        auto entryPtrOnPage = entryPos * pagedVectorVarSized->getEntrySize();
        return allPages.back().getBuffer<int8_t>() + entryPtrOnPage;
    }
    return nullptr;
}

uint64_t storeTextProxy(void* pagedVectorVarSizedPtr, const int8_t* textValue, uint32_t textSize) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    return pagedVectorVarSized->storeText(textValue, textSize);
}

int8_t* loadTextProxy(void* pagedVectorVarSizedPtr, uint64_t textEntryMapKey) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    return pagedVectorVarSized->loadText(textEntryMapKey);
}

UInt64Val PagedVectorVarSizedRef::getCapacityPerPage() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, capacityPerPage, uint64_t);
}

UInt64Val PagedVectorVarSizedRef::getTotalNumberOfEntries() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, totalNumberOfEntries, uint64_t);
}

void PagedVectorVarSizedRef::setTotalNumberOfEntries(const UInt64Val& val) {
    *getMemberAsPointer(pagedVectorVarSizedRef, PagedVectorVarSized, totalNumberOfEntries, uint64_t) = val;
}

UInt64Val PagedVectorVarSizedRef::getNumberOfEntriesOnCurrPage() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, numberOfEntriesOnCurrPage, uint64_t);
}

void PagedVectorVarSizedRef::setNumberOfEntriesOnCurrPage(const UInt64Val& val) {
    *getMemberAsPointer(pagedVectorVarSizedRef, PagedVectorVarSized, numberOfEntriesOnCurrPage, uint64_t) = val;
}

void PagedVectorVarSizedRef::writeRecord(const Record& record) {
    auto tuplesOnPage = getNumberOfEntriesOnCurrPage();
    if (tuplesOnPage >= getCapacityPerPage()) {
        nautilus::invoke(allocateNewPageVarSizedProxy, pagedVectorVarSizedRef);
        tuplesOnPage = 0;
    }

    setNumberOfEntriesOnCurrPage(tuplesOnPage + 1);
    auto oldTotalNumberOfEntries = getTotalNumberOfEntries();
    setTotalNumberOfEntries(oldTotalNumberOfEntries + 1);
    auto pageEntry = nautilus::invoke(getEntryVarSizedProxy, pagedVectorVarSizedRef, oldTotalNumberOfEntries);

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : nautilus::static_iterable(schema->fields)) {
        const auto fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        const auto fieldName = field->getName();
        const auto& fieldValue = record.read(fieldName);

        if (fieldType->type->isText()) {
            const auto textContent = std::dynamic_pointer_cast<VariableSizeExecutableDataType>(fieldValue);
            auto textEntryMapKey =
                nautilus::invoke(storeTextProxy, pagedVectorVarSizedRef, textContent->getContent(), textContent->getSize());
            writeValueToMemRef(pageEntry, textEntryMapKey, uint64_t);
            // We need casting sizeof() to a uint64 as it otherwise fails on MacOS
            pageEntry = pageEntry + UInt64Val((uint64_t) sizeof(uint64_t));
        } else {
            fieldValue->writeToMemRefVal(pageEntry);
            pageEntry = pageEntry + UInt64Val(fieldType->size());
        }
    }
}

Record PagedVectorVarSizedRef::readRecord(const UInt64Val& pos) {
    Record record;
    auto pageEntry = nautilus::invoke(getEntryVarSizedProxy, pagedVectorVarSizedRef, pos);

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : nautilus::static_iterable(schema->fields)) {
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        auto const fieldName = field->getName();

        if (fieldType->type->isText()) {
            const auto textEntryMapKey = readValueFromMemRef(pageEntry, uint64_t);
            // We need casting sizeof() to a uint64 as it otherwise fails on MacOS
            pageEntry = pageEntry + UInt64Val((uint64_t) sizeof(uint64_t));
            auto ptrToVarSized = nautilus::invoke(loadTextProxy, pagedVectorVarSizedRef, textEntryMapKey);
            const auto varSizedData = VariableSizeExecutableDataType::create(ptrToVarSized);
            record.write(fieldName, varSizedData);
        } else {
            auto fieldMemRef = pageEntry;
            auto fieldValue = readExecDataTypeFromMemRef(fieldMemRef, fieldType);
            record.write(fieldName, fieldValue);
            pageEntry = pageEntry + UInt64Val(fieldType->size());
        }
    }

    return record;
}

PagedVectorVarSizedRefIter PagedVectorVarSizedRef::begin() { return at(0); }

PagedVectorVarSizedRefIter PagedVectorVarSizedRef::at(UInt64Val pos) {
    PagedVectorVarSizedRefIter pagedVectorVarSizedRefIter(*this);
    pagedVectorVarSizedRefIter.setPos(std::move(pos));
    return pagedVectorVarSizedRefIter;
}

PagedVectorVarSizedRefIter PagedVectorVarSizedRef::end() { return at(getTotalNumberOfEntries()); }

bool PagedVectorVarSizedRef::operator==(const PagedVectorVarSizedRef& other) const {
    if (this == &other) {
        return true;
    }

    return schema == other.schema && pagedVectorVarSizedRef == other.pagedVectorVarSizedRef;
}

PagedVectorVarSizedRefIter::PagedVectorVarSizedRefIter(const PagedVectorVarSizedRef& pagedVectorVarSized)
    : pos(0), pagedVectorVarSized(pagedVectorVarSized) {}

Record PagedVectorVarSizedRefIter::operator*() { return pagedVectorVarSized.readRecord(pos); }

PagedVectorVarSizedRefIter& PagedVectorVarSizedRefIter::operator++() {
    pos = pos + 1;
    return *this;
}

bool PagedVectorVarSizedRefIter::operator==(const PagedVectorVarSizedRefIter& other) const {
    if (this == &other) {
        return true;
    }

    return pos == other.pos && pagedVectorVarSized == other.pagedVectorVarSized;
}

bool PagedVectorVarSizedRefIter::operator!=(const PagedVectorVarSizedRefIter& other) const { return !(*this == other); }

void PagedVectorVarSizedRefIter::setPos(UInt64Val newValue) { pos = newValue; }

}// namespace NES::Nautilus::Interface
