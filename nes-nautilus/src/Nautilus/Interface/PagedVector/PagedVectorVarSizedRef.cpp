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
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>

namespace NES::Nautilus::Interface {
PagedVectorVarSizedRef::PagedVectorVarSizedRef(const Value<MemRef>& pagedVectorVarSizedRef, SchemaPtr schema)
    : pagedVectorVarSizedRef(pagedVectorVarSizedRef), schema(std::move(schema)) {}

void allocateNewPageVarSizedProxy(void* pagedVectorVarSizedPtr) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    pagedVectorVarSized->appendPage();
}

void* getEntryVarSizedProxy(void* pagedVectorVarSizedPtr, uint64_t entryPos) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    auto& allPages = pagedVectorVarSized->getPages();
    for (auto& page : allPages) {
        auto numTuplesOnPage = page.getNumberOfTuples();

        if (entryPos < numTuplesOnPage) {
            auto entryPtrOnPage = entryPos * pagedVectorVarSized->getEntrySize();
            return page.getBuffer() + entryPtrOnPage;
        } else {
            entryPos -= numTuplesOnPage;
        }
    }

    // As we might have not set the number of tuples of the last tuple buffer
    if (entryPos < pagedVectorVarSized->getNumberOfEntriesOnCurrentPage()) {
        auto entryPtrOnPage = entryPos * pagedVectorVarSized->getEntrySize();
        return allPages.back().getBuffer() + entryPtrOnPage;
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

void removeEmptyTrailingPagesProxy(void* pagedVectorVarSizedPtr, uint64_t numberOfEmptyTrailingPages) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    pagedVectorVarSized->removeEmptyTrailingPages(numberOfEmptyTrailingPages);
}

void removeEmptyTrailingVarSizedPagesProxy(void* pagedVectorVarSizedPtr, uint64_t lastValidVarSizedPtr) {
    auto* pagedVectorVarSized = (PagedVectorVarSized*) pagedVectorVarSizedPtr;
    pagedVectorVarSized->removeEmptyTrailingVarSizedPages(lastValidVarSizedPtr);
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
            auto textEntryMapKey = Nautilus::FunctionCall("storeTextProxy",
                                                          storeTextProxy,
                                                          pagedVectorVarSizedRef,
                                                          fieldValue.as<Text>()->getReference());
            pageEntry.as<MemRef>().store(textEntryMapKey.as<UInt64>());
            // We need casting sizeof() to a uint64 as it otherwise fails on MacOS
            pageEntry = pageEntry + Value<UInt64>((uint64_t) sizeof(uint64_t));
        } else {
            pageEntry.as<MemRef>().store(fieldValue);
            pageEntry = pageEntry + fieldType->size();
        }
    }
}

void PagedVectorVarSizedRef::writeRecordThatAlreadyHasTextPartStored(Record record) {
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
            pageEntry.as<MemRef>().store(fieldValue.as<UInt64>());
            // We need casting sizeof() to a uint64 as it otherwise fails on MacOS
            pageEntry = pageEntry + Value<UInt64>((uint64_t) sizeof(uint64_t));
        } else {
            pageEntry.as<MemRef>().store(fieldValue);
            pageEntry = pageEntry + fieldType->size();
        }
    }
}

std::vector<Value<UInt64>> PagedVectorVarSizedRef::readTextPointers(const Value<UInt64>& pos) {
    std::vector<Value<UInt64>> res;
    auto pageEntry = Nautilus::FunctionCall("getEntryVarSizedProxy", getEntryVarSizedProxy, pagedVectorVarSizedRef, pos);
    DefaultPhysicalTypeFactory physicalDataTypeFactory;

    for (auto& field : schema->fields) {
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        if (fieldType->type->isText()) {
            auto textEntryMapKey = pageEntry.as<MemRef>().load<UInt64>();
            // We need casting sizeof() to a uint64 as it otherwise fails on MacOS
            pageEntry = pageEntry + Value<UInt64>((uint64_t) sizeof(uint64_t));
            res.push_back(textEntryMapKey);
        } else {
            pageEntry = pageEntry + fieldType->size();
        }
    }

    return res;
}

Record PagedVectorVarSizedRef::readRecord(const Value<UInt64>& pos) {
    Record record;
    auto pageEntry = Nautilus::FunctionCall("getEntryVarSizedProxy", getEntryVarSizedProxy, pagedVectorVarSizedRef, pos);

    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        auto const fieldName = field->getName();

        if (fieldType->type->isText()) {
            auto textEntryMapKey = pageEntry.as<MemRef>().load<UInt64>();
            // We need casting sizeof() to a uint64 as it otherwise fails on MacOS
            pageEntry = pageEntry + Value<UInt64>((uint64_t) sizeof(uint64_t));
            auto text = Nautilus::FunctionCall("loadTextProxy", loadTextProxy, pagedVectorVarSizedRef, textEntryMapKey);
            record.write(fieldName, text);
        } else {
            auto fieldMemRef = pageEntry.as<MemRef>();
            auto fieldValue = MemRefUtils::loadValue(fieldMemRef, fieldType);
            record.write(fieldName, fieldValue);
            pageEntry = pageEntry + fieldType->size();
        }
    }
    return record;
}

Record PagedVectorVarSizedRef::readRecordNoText(const Value<UInt64>& pos) {
    Record record;
    auto pageEntry = Nautilus::FunctionCall("getEntryVarSizedProxy", getEntryVarSizedProxy, pagedVectorVarSizedRef, pos);

    DefaultPhysicalTypeFactory physicalDataTypeFactory;

    for (auto& field : schema->fields) {
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        auto const fieldName = field->getName();

        if (fieldType->type->isText()) {
            auto textEntryMapKey = pageEntry.as<MemRef>().load<UInt64>();
            // We need casting sizeof() to a uint64 as it otherwise fails on MacOS
            pageEntry = pageEntry + Value<UInt64>((uint64_t) sizeof(uint64_t));
            record.write(fieldName, textEntryMapKey);
        } else {
            auto fieldMemRef = pageEntry.as<MemRef>();
            auto fieldValue = MemRefUtils::loadValue(fieldMemRef, fieldType);
            record.write(fieldName, fieldValue);
            pageEntry = pageEntry + fieldType->size();
        }
    }
    return record;
}

void PagedVectorVarSizedRef::removeRecordsAndAdjustPositions(const std::vector<Value<UInt64>> positionsToRemove,
                                                             bool keepStrings) {
    if (positionsToRemove.empty()) {
        return;
    }

    NES_INFO("Removing {} records from this slice", positionsToRemove.size())

    // the position vector gets constructed in the way that the slice is being looked through from the end to the beginning.
    // Therefore, the position of the last tuple that was added to this vector, is the smallest position.
    auto startPos = positionsToRemove[positionsToRemove.size() - 1];

    // If the pagedVector contains text fields, we will remove all stored text starting from the first text field of the first
    // removed record.
    auto containsVarSizedFields = false;
    auto firstInvalidVarSizedPtr = Value<UInt64>(0_u64);
    for (auto field : schema->fields) {
        if (field->getDataType()->isText()) {
            containsVarSizedFields = true;
            auto ptrs = readTextPointers(startPos);
            firstInvalidVarSizedPtr = ptrs[0];
            break;
        }
    }

    NES_DEBUG("StartPos {}", startPos->getValue())
    // Store all records that should not be removed. These records don't stay at the same position but should move "up" in the page,
    // so each valid record is directly after another valid one in memory
    std::vector<Record> recordsStayingChangingPosition;
    for (auto i = startPos; i < getTotalNumberOfEntries(); i = i + 1) {
        if (std::find(positionsToRemove.begin(), positionsToRemove.end(), i) == positionsToRemove.end()) {
            if (keepStrings) {
                recordsStayingChangingPosition.push_back(readRecordNoText(i));
            } else {
                recordsStayingChangingPosition.push_back(readRecord(i));
            }
        }
    }

    auto capacityPage = getCapacityPerPage();
    auto tuplesCurrentPage = getNumberOfEntriesOnCurrPage();

    auto numberOfRecordsRemovedOrChanged =
        Value<UInt64>((uint64_t) positionsToRemove.size() + (uint64_t) recordsStayingChangingPosition.size());//casting for macOs
    // Adjust total number of tuples and number of tuples on the current page
    setTotalNumberOfEntries(getTotalNumberOfEntries() - numberOfRecordsRemovedOrChanged);
    // If we remove and adjust the position of fewer records than currently on the page, we can simply set the number of records on
    // this page, so new records will be written from that position.
    if (numberOfRecordsRemovedOrChanged <= tuplesCurrentPage) {
        setNumberOfEntriesOnCurrPage(tuplesCurrentPage - numberOfRecordsRemovedOrChanged);
    }
    // Otherwise we remove pages that won't contain any records anymore and set the numberEntriesOnCurrentPage for the first page
    // that still contains records
    else {
        auto numberOfNowEmptyPages = Value<UInt64>(1_u64);
        auto numberOfRemovedTuples = numberOfRecordsRemovedOrChanged - tuplesCurrentPage;
        while (numberOfRemovedTuples > capacityPage) {
            numberOfRemovedTuples = numberOfRemovedTuples - capacityPage;
            numberOfNowEmptyPages = numberOfNowEmptyPages + 1;
        }
        //remove pages from page vector
        auto emptyPages = Value<UInt64>(numberOfNowEmptyPages);
        Nautilus::FunctionCall("removeEmptyTrailingPagesProxy",
                               removeEmptyTrailingPagesProxy,
                               pagedVectorVarSizedRef,
                               emptyPages);
        //first remove empty trailing pages and then set the number of entries on the current page (that has changed now)
        setNumberOfEntriesOnCurrPage(capacityPage - numberOfRemovedTuples);
    }

    NES_INFO("Total number of records that stay on the pagedVector {}", getTotalNumberOfEntries()->getValue())
    NES_INFO("Number of records that stay on the last (current) page = (tuplesOnPage - removed % capacity)  {} - {} % {} = {}",
             tuplesCurrentPage->getValue(),
             positionsToRemove.size(),
             capacityPage->getValue(),
             getNumberOfEntriesOnCurrPage()->getValue())

    if (containsVarSizedFields && !keepStrings) {
        Nautilus::FunctionCall("removeEmptyTrailingVarSizedPagesProxy",
                               removeEmptyTrailingVarSizedPagesProxy,
                               pagedVectorVarSizedRef,
                               firstInvalidVarSizedPtr);
    }

    for (Record record : recordsStayingChangingPosition) {
        if (keepStrings) {
            writeRecordThatAlreadyHasTextPartStored(record);
        } else {
            writeRecord(record);
        }
    }
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

}// namespace NES::Nautilus::Interface
