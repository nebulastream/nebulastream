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
#include <Common/DataTypes/Boolean.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRowLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>

namespace NES::Nautilus::Interface {
uint64_t pagedVectorIteratorLoadProxy(void* iteratorPtr) {
    auto it = static_cast<Runtime::PagedVectorRowLayout::EntryIterator*>(iteratorPtr);
    return std::bit_cast<uint64_t>(it->operator*().getRef());
}
void pagedVectorIteratorIncProxy(void* iteratorPtr) {
    static_cast<Runtime::PagedVectorRowLayout::EntryIterator*>(iteratorPtr)->operator++();
}
bool pagedVectorIteratorNotEqualsProxy(void* iteratorPtr, void* iteratorOtherPtr) {
    return *static_cast<Runtime::PagedVectorRowLayout::EntryIterator*>(iteratorPtr)
        != *static_cast<Runtime::PagedVectorRowLayout::EntryIterator*>(iteratorOtherPtr);
}
bool pagedVectorIteratorEqualsProxy(void* iteratorPtr, void* iteratorOtherPtr) {
    return *static_cast<Runtime::PagedVectorRowLayout::EntryIterator*>(iteratorPtr)
        == *static_cast<Runtime::PagedVectorRowLayout::EntryIterator*>(iteratorOtherPtr);
}
void releasePagedVectorIterator(void* iteratorPtr) {
    delete static_cast<Runtime::PagedVectorRowLayout::EntryIterator*>(iteratorPtr);
}

uint64_t allocateEntryProxy(void* pagedVectorRef) {
    auto pagedVector = static_cast<Runtime::PagedVectorRowLayout*>(pagedVectorRef);
    return std::bit_cast<uint64_t>(pagedVector->allocateEntry().getRef());
}

void* entryDataProxy(void* pagedVectorRef, uint64_t entryRef) {
    auto pagedVector = static_cast<Runtime::PagedVectorRowLayout*>(pagedVectorRef);
    auto ref = std::bit_cast<Runtime::EntryRef>(entryRef);
    auto entry = pagedVector->getEntry(ref);
    auto entryMemory = entry.data();
    return entryMemory.data();
}

void* storeVarSizedDataProxy(void* pagedVectorRef, uint64_t entryRef, void* offset, TextValue* text) {
    auto pagedVector = static_cast<Runtime::PagedVectorRowLayout*>(pagedVectorRef);
    auto ref = std::bit_cast<Runtime::EntryRef>(entryRef);
    auto entry = pagedVector->getEntry(ref);
    return entry.storeVarSizedDataToOffset(std::span{text->c_str(), text->length()}, offset);
}

std::tuple<TextValue*, void*> loadVarSizedData(void* pagedVectorRef, uint64_t entryRef, void* offset) {
    auto pagedVector = static_cast<Runtime::PagedVectorRowLayout*>(pagedVectorRef);
    auto ref = std::bit_cast<Runtime::EntryRef>(entryRef);
    auto entry = pagedVector->getEntry(ref);
    return entry.loadVarSizedDataAtOffset(offset);
}

PagedVectorVarSizedRefIterator::PagedVectorVarSizedRefIterator(Value<MemRef> iteratorPtr,
                                                               SchemaPtr schema,
                                                               Value<MemRef> pagedVectorPtr)
    : iteratorPtr(std::move(iteratorPtr)), schema(std::move(schema)), pagedVectorPtr(std::move(pagedVectorPtr)) {}
PagedVectorVarSizedRefIterator::~PagedVectorVarSizedRefIterator() {
    FunctionCall("releasePagedVectorIterator", releasePagedVectorIterator, iteratorPtr);
}

PagedVectorVarSizedRefIterator::reference PagedVectorVarSizedRefIterator::operator*() const {
    auto handle = FunctionCall("pagedVectorIteratorLoadProxy", pagedVectorIteratorLoadProxy, iteratorPtr);
    auto entryData = FunctionCall("entryDataProxy", entryDataProxy, pagedVectorPtr, handle);

    Record record;
    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        auto const fieldName = field->getName();
        if (fieldType->type->isText()) {
            auto [text, new_entry] =
                Nautilus::FunctionCall("loadVarSizedData", loadVarSizedData, pagedVectorPtr, handle, entryData);
            entryData = new_entry;
            record.write(fieldName, text);
        } else {
            auto fieldMemRef = entryData.as<MemRef>();
            auto fieldValue = MemRefUtils::loadValue(fieldMemRef, fieldType);
            record.write(fieldName, fieldValue);
            entryData = entryData + fieldType->size();
        }
    }

    return record;
}
PagedVectorVarSizedRefIterator& PagedVectorVarSizedRefIterator::operator++() {
    FunctionCall("pagedVectorIteratorIncProxy", pagedVectorIteratorIncProxy, iteratorPtr);
    return *this;
}
PagedVectorVarSizedRef::PagedVectorVarSizedRef(const Value<MemRef>& pagedVectorVarSizedRef, SchemaPtr schema)
    : pagedVectorVarSizedRef(pagedVectorVarSizedRef), schema(std::move(schema)) {}

void PagedVectorVarSizedRef::writeRecord(Record record) {
    Value<UInt64> handle = FunctionCall("allocateEntryProxy", allocateEntryProxy, pagedVectorVarSizedRef);
    Value<MemRef> entryData = FunctionCall("entryDataProxy", entryDataProxy, pagedVectorVarSizedRef, handle);

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        auto const fieldName = field->getName();
        auto const fieldValue = record.read(fieldName);

        if (fieldType->type->isText()) {
            entryData = Nautilus::FunctionCall("storeVarSizedDataProxy",
                                               storeVarSizedDataProxy,
                                               pagedVectorVarSizedRef,
                                               handle,
                                               entryData,
                                               fieldValue.as<Text>()->getReference());
        } else {
            entryData.as<MemRef>().store(fieldValue);
            entryData = entryData + fieldType->size();
        }
    }
}

void* pagedSizeVectorBeginProxy(void* pagedVectorPtr) {
    auto pagedVector = static_cast<Runtime::PagedVectorRowLayout*>(pagedVectorPtr);
    return pagedVector->beginAlloc();
}

void* pagedSizeVectorEndProxy(void* pagedVectorPtr) {
    auto pagedVector = static_cast<Runtime::PagedVectorRowLayout*>(pagedVectorPtr);
    return pagedVector->endAlloc();
}

Value<UInt64> PagedVectorVarSizedRef::getTotalNumberOfEntries() {
    return getMember(pagedVectorVarSizedRef, Runtime::PagedVectorRowLayout, numberOfEntries).load<UInt64>();
}
PagedVectorVarSizedRefIterator PagedVectorVarSizedRef::begin() {
    auto iteratorPtr = FunctionCall("pagedSizeVectorBeginProxy", pagedSizeVectorBeginProxy, pagedVectorVarSizedRef);
    return PagedVectorVarSizedRefIterator(iteratorPtr, schema, pagedVectorVarSizedRef);
}

PagedVectorVarSizedRefIterator PagedVectorVarSizedRef::end() {
    auto iteratorPtr = FunctionCall("pagedSizeVectorEndProxy", pagedSizeVectorEndProxy, pagedVectorVarSizedRef);
    return PagedVectorVarSizedRefIterator(iteratorPtr, schema, pagedVectorVarSizedRef);
}

bool PagedVectorVarSizedRef::operator==(const PagedVectorVarSizedRef& other) const {
    if (this == &other) {
        return true;
    }

    return schema == other.schema && pagedVectorVarSizedRef == other.pagedVectorVarSizedRef;
}

Value<Boolean> operator==(const PagedVectorVarSizedRefIterator& a, const PagedVectorVarSizedRefIterator& b) {
    return FunctionCall("pagedVectorIteratorEquals", pagedVectorIteratorEqualsProxy, a.iteratorPtr, b.iteratorPtr);
}
Value<Boolean> operator!=(const PagedVectorVarSizedRefIterator& a, const PagedVectorVarSizedRefIterator& b) {
    return FunctionCall("pagedVectorIteratorNotEquals", pagedVectorIteratorNotEqualsProxy, a.iteratorPtr, b.iteratorPtr);
}
}// namespace NES::Nautilus::Interface
