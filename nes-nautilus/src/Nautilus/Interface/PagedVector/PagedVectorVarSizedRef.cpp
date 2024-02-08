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

#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <API/AttributeField.hpp>

namespace NES::Nautilus::Interface {
PagedVectorVarSizedRef::PagedVectorVarSizedRef(const Value<MemRef>& pagedVectorVarSizedRef, SchemaPtr schema)
    : pagedVectorVarSizedRef(pagedVectorVarSizedRef), schema(std::move(schema)) {}

void* getPageProxy(void* pagedVectorVarSizedPtr, uint64_t pagePos) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    return pagedVectorVarSized->getPages()[pagePos].getBuffer();
}

void allocateNewPageProxy(void* pagedVectorVarSizedPtr) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    pagedVectorVarSized->appendPage();
}

void storeTextProxy(void* pagedVectorVarSizedPtr, TextValue* textValue) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    pagedVectorVarSized->storeText(textValue->c_str(), textValue->length());
}

TextValue* loadTextProxy(void* pagedVectorVarSizedPtr, void* textValuePtr, uint32_t textLength) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    return TextValue::create(pagedVectorVarSized->loadText(static_cast<uint8_t*>(textValuePtr), textLength));
}

Value<UInt64> PagedVectorVarSizedRef::getCapacityPerPage() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, capacityPerPage).load<UInt64>();
}

Value<MemRef> PagedVectorVarSizedRef::getCurrentVarSizedDataEntry() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, currVarSizedDataEntry).load<MemRef>();
}

Value<UInt64> PagedVectorVarSizedRef::getEntrySize() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, entrySize).load<UInt64>();
}

Value<UInt64> PagedVectorVarSizedRef::getTotalNumberOfEntries() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, totalNumberOfEntries).load<UInt64>();
}

void PagedVectorVarSizedRef::setTotalNumberOfEntries(const Value<>& val) {
    getMember(pagedVectorVarSizedRef, PagedVectorVarSized, totalNumberOfEntries).store(val);
}

void PagedVectorVarSizedRef::writeRecord(Record record) {
    auto capacityPerPage = getCapacityPerPage();
    auto totalNumberOfEntries = getTotalNumberOfEntries();
    auto pagePos = (totalNumberOfEntries / capacityPerPage).as<UInt64>();
    auto tuplesOnPage = (totalNumberOfEntries - pagePos * capacityPerPage).as<UInt64>();

    if (tuplesOnPage >= capacityPerPage) {
        Nautilus::FunctionCall("allocateNewPageProxy", allocateNewPageProxy, pagedVectorVarSizedRef);
        pagePos = (pagePos + 1_u64).as<UInt64>();
        tuplesOnPage = (tuplesOnPage - capacityPerPage).as<UInt64>();
    }
    auto pageBase = Nautilus::FunctionCall("getPageProxy", getPageProxy, pagedVectorVarSizedRef, pagePos);
    auto pageEntry = pageBase + (tuplesOnPage * getEntrySize());

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        auto const fieldName = field->getName();
        auto const fieldValue = record.read(fieldName);

        if (fieldType->type->isText()) {
            // TODO check sizeof operator
            pageEntry.as<MemRef>().store(getCurrentVarSizedDataEntry());
            pageEntry = pageEntry + sizeof(MemRef);
            pageEntry.as<MemRef>().store(fieldValue.as<Text>()->length());
            pageEntry = pageEntry + sizeof(UInt32);

            Nautilus::FunctionCall("storeTextProxy", storeTextProxy, pagedVectorVarSizedRef, fieldValue.as<Text>()->getReference());
        } else {
            pageEntry.as<MemRef>().store(fieldValue);
            pageEntry = pageEntry + fieldType->size();
        }
    }
    setTotalNumberOfEntries(totalNumberOfEntries + 1_u64);
}

Record PagedVectorVarSizedRef::readRecord(const Value<UInt64>& pos) {
    Record record;
    // get memref to entry at pos
    // use schema fields to retrieve values from entry
    // use schema fields to get fieldName
    // if text -> get varSizedData from the ptr and size in entry and copy to record
    // else -> copy value to record
    return record;
}

PagedVectorVarSizedRefIter PagedVectorVarSizedRef::begin() { return at(0_u64); }

PagedVectorVarSizedRefIter PagedVectorVarSizedRef::at(Value<UInt64> pos) {
    PagedVectorVarSizedRefIter pagedVectorVarSizedRefIter(*this);
    pagedVectorVarSizedRefIter.setPos(pos);
    return pagedVectorVarSizedRefIter;
}

PagedVectorVarSizedRefIter PagedVectorVarSizedRef::end() { return at(this->getTotalNumberOfEntries()); }

bool PagedVectorVarSizedRef::operator==(const PagedVectorVarSizedRef& other) const {
    if (this == &other) {
        return true;
    }

    return schema == other.schema && pagedVectorVarSizedRef == other.pagedVectorVarSizedRef;
}

PagedVectorVarSizedRefIter::PagedVectorVarSizedRefIter(const PagedVectorVarSizedRef& pagedVectorVarSized)
    : pos(0_u64), pagedVectorVarSized(pagedVectorVarSized) {}

PagedVectorVarSizedRefIter::PagedVectorVarSizedRefIter(const PagedVectorVarSizedRefIter& it)
    : pos(it.pos), pagedVectorVarSized(it.pagedVectorVarSized) {}

PagedVectorVarSizedRefIter& PagedVectorVarSizedRefIter::operator=(const PagedVectorVarSizedRefIter& it) {
    if (this == &it) {
        return *this;
    }

    pos = it.pos;
    // TODO schema has no copy assignment operator
    pagedVectorVarSized = it.pagedVectorVarSized;
    return *this;
}

Record PagedVectorVarSizedRefIter::operator*() { return pagedVectorVarSized.readRecord(pos); }

PagedVectorVarSizedRefIter& PagedVectorVarSizedRefIter::operator++() {
    pos = pos + 1;
    return *this;
}

PagedVectorVarSizedRefIter PagedVectorVarSizedRefIter::operator++(int) {
    PagedVectorVarSizedRefIter copy = *this;
    pos = pos + 1;
    return copy;
}

bool PagedVectorVarSizedRefIter::operator==(const PagedVectorVarSizedRefIter& other) const {
    if (this == &other) {
        return true;
    }

    return pos == other.pos && pagedVectorVarSized == other.pagedVectorVarSized;
}

bool PagedVectorVarSizedRefIter::operator!=(const PagedVectorVarSizedRefIter& other) const { return !(*this == other); }

void PagedVectorVarSizedRefIter::setPos(Value<UInt64> newValue) { pos = newValue; }

} //NES::Nautilus::Interface
