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
            Nautilus::FunctionCall("storeTextProxy", storeTextProxy, pagedVectorVarSizedRef, fieldValue.as<Text>()->getReference());

            // TODO check sizeof operator
            auto textLength = fieldValue.as<Text>()->length();
            auto textPtr = (getCurrentVarSizedDataEntry() - textLength).as<MemRef>();
            pageEntry.as<MemRef>().store(textPtr);
            pageEntry = pageEntry + sizeof(MemRef);
            pageEntry.as<MemRef>().store(textLength);
            pageEntry = pageEntry + sizeof(UInt32);
        } else {
            pageEntry.as<MemRef>().store(fieldValue);
            pageEntry = pageEntry + fieldType->size();
        }
    }
    setTotalNumberOfEntries(totalNumberOfEntries + 1_u64);
}

Record PagedVectorVarSizedRef::readRecord(const Value<UInt64>& pos) {
    Record record;

    auto capacityPerPage = getCapacityPerPage();
    auto totalNumberOfEntries = getTotalNumberOfEntries();
    auto pagePos = (pos / capacityPerPage).as<UInt64>();
    auto positionOnPage = (pos - pagePos * capacityPerPage).as<UInt64>();

    auto pageBase = Nautilus::FunctionCall("getPageProxy", getPageProxy, pagedVectorVarSizedRef, pagePos);
    auto pageEntry = pageBase + positionOnPage * getEntrySize();

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        auto const fieldName = field->getName();

        if (fieldType->type->isText()) {
            // TODO check sizeof operator
            auto textPtr = pageEntry.as<MemRef>().load<MemRef>();
            pageEntry = pageEntry + sizeof(MemRef);
            auto textLength = pageEntry.as<MemRef>().load<UInt32>();
            pageEntry = pageEntry + sizeof(UInt32);

            auto text = Nautilus::FunctionCall("loadTextProxy", loadTextProxy, pagedVectorVarSizedRef, textPtr, textLength);
            record.write(fieldName, text);
        } else {
            auto fieldValue = loadBasicType(fieldType, pageEntry.as<MemRef>());
            record.write(fieldName, fieldValue);
            pageEntry = pageEntry + fieldType->size();
        }
    }
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

Value<> PagedVectorVarSizedRef::loadBasicType(const PhysicalTypePtr& type,
                                                        Value<MemRef> fieldReference) {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return fieldReference.load<Nautilus::Boolean>();
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return fieldReference.load<Nautilus::Int8>();
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return fieldReference.load<Nautilus::Int16>();
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return fieldReference.load<Nautilus::Int32>();
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return fieldReference.load<Nautilus::Int64>();
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return fieldReference.load<Nautilus::UInt8>();
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return fieldReference.load<Nautilus::UInt16>();
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return fieldReference.load<Nautilus::UInt32>();
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return fieldReference.load<Nautilus::UInt64>();
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return fieldReference.load<Nautilus::Float>();
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return fieldReference.load<Nautilus::Double>();
            };
            case BasicPhysicalType::NativeType::TEXT: {
                // TODO implement here or in readRecord?
                NES_ERROR("PagedVectorVarSizedRef::loadBasicType: Physical Type: {} should be handled elsewhere",
                          type->toString());
            };
            default: {
                NES_ERROR("PagedVectorVarSizedRef::loadBasicType: Physical Type: {} is currently not supported",
                          type->toString());
                NES_NOT_IMPLEMENTED();
            };
        }
    }
    NES_NOT_IMPLEMENTED();
}

PagedVectorVarSizedRefIter::PagedVectorVarSizedRefIter(const PagedVectorVarSizedRef& pagedVectorVarSized)
    : pos(0_u64), pagedVectorVarSized(pagedVectorVarSized) {}

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

void PagedVectorVarSizedRefIter::setPos(Value<UInt64> newValue) { pos = newValue; }

} //NES::Nautilus::Interface
