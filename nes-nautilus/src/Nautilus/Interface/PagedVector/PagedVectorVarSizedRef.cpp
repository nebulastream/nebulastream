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

//PagedVectorVarSizedRef::PagedVectorVarSizedRef(const PagedVectorVarSizedRef& other)
//    : pagedVectorVarSizedRef(other.pagedVectorVarSizedRef), schema(other.schema->copy()) {}

void allocateNewPageVarSizedProxy(void* pagedVectorVarSizedPtr) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    pagedVectorVarSized->appendPage();
}

void* getEntryVarSizedProxy(void* pagedVectorVarSizedPtr, uint64_t entryPos) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    for (auto& page : pagedVectorVarSized->getPages()) {
        auto numTuplesOnPage = page.getNumberOfTuples();

        if (entryPos < numTuplesOnPage || entryPos < pagedVectorVarSized->getNumberOfEntriesOnCurrentPage()) {
            auto entryPtrOnPage = entryPos * pagedVectorVarSized->getEntrySize();
            return page.getBuffer() + entryPtrOnPage;
        } else {
            entryPos -= numTuplesOnPage;
        }
    }
    return nullptr;
}

uint64_t storeTextProxy(void* pagedVectorVarSizedPtr, TextValue* textValue) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    return pagedVectorVarSized->storeText(textValue->c_str(), textValue->length());
}

TextValue* loadTextProxy(void* pagedVectorVarSizedPtr, uint64_t textEntryMapKey) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    return pagedVectorVarSized->loadText(textEntryMapKey);
}

Value<UInt64> PagedVectorVarSizedRef::getCapacityPerPage() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, capacityPerPage).load<UInt64>();
}

Value<UInt64> PagedVectorVarSizedRef::getTotalNumberOfEntries() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, totalNumberOfEntries).load<UInt64>();
}

void PagedVectorVarSizedRef::setTotalNumberOfEntries(const Value<>& val) {
    getMember(pagedVectorVarSizedRef, PagedVectorVarSized, totalNumberOfEntries).store(val);
}

Value<UInt64> PagedVectorVarSizedRef::getNumberOfEntriesOnCurrPage() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, numberOfEntriesOnCurrPage).load<UInt64>();
}

void PagedVectorVarSizedRef::setNumberOfEntriesOnCurrPage(const Value<>& val) {
    getMember(pagedVectorVarSizedRef, PagedVectorVarSized, numberOfEntriesOnCurrPage).store(val);
}

void PagedVectorVarSizedRef::writeRecord(Record record) {
    auto tuplesOnPage = getNumberOfEntriesOnCurrPage();
    if (tuplesOnPage >= getCapacityPerPage()) {
        Nautilus::FunctionCall("allocateNewPageVarSizedProxy", allocateNewPageVarSizedProxy, pagedVectorVarSizedRef);
        tuplesOnPage = 0_u64;
    }

    setNumberOfEntriesOnCurrPage(tuplesOnPage + 1_u64);
    auto oldTotalNumberOfEntries = getTotalNumberOfEntries();
    setTotalNumberOfEntries(oldTotalNumberOfEntries + 1_u64);
    auto pageEntry =
        Nautilus::FunctionCall("getEntryVarSizedProxy", getEntryVarSizedProxy, pagedVectorVarSizedRef, oldTotalNumberOfEntries);

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        auto const fieldName = field->getName();
        auto const fieldValue = record.read(fieldName);

        if (fieldType->type->isText()) {
            auto textEntryMapKey =
                Nautilus::FunctionCall("storeTextProxy", storeTextProxy, pagedVectorVarSizedRef, fieldValue.as<Text>()->getReference());
            pageEntry.as<MemRef>().store(textEntryMapKey.as<UInt64>());
            pageEntry = pageEntry + sizeof(uint64_t);
        } else {
            pageEntry.as<MemRef>().store(fieldValue);
            pageEntry = pageEntry + fieldType->size();
        }
    }
}

Record PagedVectorVarSizedRef::readRecord(const Value<UInt64>& pos) {
    Record record;
    //if (pos < getTotalNumberOfEntries()) {
        auto pageEntry = Nautilus::FunctionCall("getEntryVarSizedProxy", getEntryVarSizedProxy, pagedVectorVarSizedRef, pos);

        DefaultPhysicalTypeFactory physicalDataTypeFactory;
        for (auto& field : schema->fields) {
            auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
            auto const fieldName = field->getName();

            if (fieldType->type->isText()) {
                auto textEntryMapKey = pageEntry.as<MemRef>().load<UInt64>();
                pageEntry = pageEntry + sizeof(uint64_t);
                auto text = Nautilus::FunctionCall("loadTextProxy", loadTextProxy, pagedVectorVarSizedRef, textEntryMapKey);
                record.write(fieldName, text);
            } else {
                auto fieldValue = loadBasicType(fieldType, pageEntry.as<MemRef>());
                record.write(fieldName, fieldValue);
                pageEntry = pageEntry + fieldType->size();
            }
        }
    //} else {
        //    NES_ERROR("PagedVectorVarSizedRef::readRecord: Entry with index {} does not exist!", pos->toString());
        //}
    return record;
}

PagedVectorVarSizedRefIter PagedVectorVarSizedRef::begin() { return at(0_u64); }

PagedVectorVarSizedRefIter PagedVectorVarSizedRef::at(Value<UInt64> pos) {
    PagedVectorVarSizedRefIter pagedVectorVarSizedRefIter(*this);
    pagedVectorVarSizedRefIter.setPos(pos);
    return pagedVectorVarSizedRefIter;
}

PagedVectorVarSizedRefIter PagedVectorVarSizedRef::end() { return at(getTotalNumberOfEntries()); }

bool PagedVectorVarSizedRef::operator==(const PagedVectorVarSizedRef& other) const {
    if (this == &other) {
        return true;
    }

    return schema == other.schema && pagedVectorVarSizedRef == other.pagedVectorVarSizedRef;
}
//
//PagedVectorVarSizedRef& PagedVectorVarSizedRef::operator=(const PagedVectorVarSizedRef& other) {
//    if (this == &other) {
//        return *this;
//    }
//    pagedVectorVarSizedRef = other.pagedVectorVarSizedRef;
//    schema->copyFields(other.schema);
//    return *this;
//}

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
