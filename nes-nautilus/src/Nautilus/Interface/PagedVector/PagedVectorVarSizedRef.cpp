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
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <API/AttributeField.hpp>
#include <utility>

namespace NES::Nautilus::Interface {
PagedVectorVarSizedRef::PagedVectorVarSizedRef(const Value<NES::Nautilus::MemRef>& pagedVectorVarSizedRef, SchemaPtr schema)
    : pagedVectorVarSizedRef(pagedVectorVarSizedRef), schema(std::move(schema)) {}

void allocateNewPageProxy(void* pagedVectorVarSizedPtr) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    pagedVectorVarSized->appendPage();
}

void storeTextProxy(void* pagedVectorVarSizedPtr, Nautilus::TextValue* textValue) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    pagedVectorVarSized->storeText(textValue->str(), textValue->length());
}

uint64_t getNoTuplesProxy(void* tupleBufferPtr) {
    auto* tupleBuffer = (Runtime::TupleBuffer*) tupleBufferPtr;
    return tupleBuffer->getNumberOfTuples();
}

void setNoTuplesProxy(void* tupleBufferPtr, uint64_t noTuples) {
    auto* tupleBuffer = (Runtime::TupleBuffer*) tupleBufferPtr;
    tupleBuffer->setNumberOfTuples(noTuples);
}

void* getBufferProxy(void* tupleBufferPtr) {
    auto* tupleBuffer = (Runtime::TupleBuffer*) tupleBufferPtr;
    return tupleBuffer->getBuffer();
}

Value<MemRef> PagedVectorVarSizedRef::getCurrentPage() {
    return getMember(pagedVectorVarSizedRef, PagedVectorVarSized, currPage).load<MemRef>();
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

void PagedVectorVarSizedRef::writeRecord(Record record) {
    auto currPage = getCurrentPage();
    auto noTuples = Nautilus::FunctionCall("getNoTuplesProxy", getNoTuplesProxy, currPage);

    if (noTuples >= getCapacityPerPage()) {
        Nautilus::FunctionCall("allocateNewPageProxy", allocateNewPageProxy, pagedVectorVarSizedRef);
        currPage = getCurrentPage();
    }
    auto pageBase = Nautilus::FunctionCall("getBufferProxy", getBufferProxy, currPage);
    auto pageEntry = pageBase + (noTuples * getEntrySize());

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto const fieldName = field->getName();
        auto const fieldValue = record.read(fieldName);
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        if (fieldType->type->isText()) {
            pageEntry.as<MemRef>().store(getCurrentVarSizedDataEntry());
            pageEntry = pageEntry + sizeof(UInt8*);
            pageEntry.as<MemRef>().store(fieldValue.as<Text>()->length());
            pageEntry = pageEntry + sizeof(UInt32);

            Nautilus::FunctionCall("storeTextProxy", storeTextProxy, pagedVectorVarSizedRef, fieldValue.as<Text>()->getReference());
        } else {
            pageEntry.as<MemRef>().store(fieldValue);
            pageEntry = pageEntry + fieldType->size();
        }
    }
    auto newNoTuples = (noTuples + 1_u64).as<UInt64>();
    Nautilus::FunctionCall("setNoTuplesProxy", setNoTuplesProxy, currPage, newNoTuples);
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

} //NES::Nautilus::Interface
