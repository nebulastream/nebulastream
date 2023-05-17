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
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/SequentialData/SequentialData.hpp>
#include <Nautilus/Interface/SequentialData/SequentialDataRef.hpp>

namespace NES::Nautilus::Interface {
SequentialDataRef::SequentialDataRef(const Value<MemRef>& sequentialDataRef, uint64_t entrySize)
    : sequentialDataRef(sequentialDataRef), entrySize(entrySize), entriesPerPage(SequentialData::PAGE_SIZE / entrySize) {}

void allocateNewPageProxy(void* sequentialDataPtr) {
    auto* sequentialData = (SequentialData*) sequentialDataPtr;
    sequentialData->appendPage();
}

void* getSequentialDataPageProxy(void* sequentialDataPtr, uint64_t pagePos) {
    auto* sequentialData = (SequentialData*) sequentialDataPtr;
    return sequentialData->getPages()[pagePos];
}

Value<MemRef> SequentialDataRef::allocateEntry() {
    // check if we should allocate a new page
    if (getNumberOfEntries() >= entriesPerPage) {
        FunctionCall("allocateNewPageProxy", allocateNewPageProxy, sequentialDataRef);
    }
    // gets the current page and reserve space for the new entry.
    auto page = getCurrentPage();
    auto entry = page + (getNumberOfEntries() * entrySize);
    setNumberOfEntries(getNumberOfEntries() + (uint64_t) 1);
    setNumberOfTotalEntries(getTotalNumberOfEntries() + (uint64_t) 1);
    return entry.as<MemRef>();
}

Value<MemRef> SequentialDataRef::getCurrentPage() { return getMember(sequentialDataRef, SequentialData, currentPage).load<MemRef>(); }

Value<UInt64> SequentialDataRef::getNumberOfEntries() { return getMember(sequentialDataRef, SequentialData, numberOfEntries).load<UInt64>(); }

Value<UInt64> SequentialDataRef::getTotalNumberOfEntries() { return getMember(sequentialDataRef, SequentialData, totalNumberOfEntries).load<UInt64>(); }

Value<MemRef> SequentialDataRef::getPage(const Value<>& pos) {
    return (getMember(sequentialDataRef, SequentialData, firstPage).load<MemRef>() + (pos * 8)).as<MemRef>();
}

Value<MemRef> SequentialDataRef::getEntry(const Value<UInt64>& pos) {
    auto pagePos = (pos / entriesPerPage).as<UInt64>();
    auto positionOnPage = pos - (pagePos * entriesPerPage);

    // TODO change this later to
    auto page = Nautilus::FunctionCall("getSequentialDataPageProxy", getSequentialDataPageProxy, sequentialDataRef, Value<UInt64>(pagePos));
    auto ptrOnPage = (positionOnPage * entrySize);
    auto retPos = page + ptrOnPage;
    return retPos.as<MemRef>();
}

void SequentialDataRef::setNumberOfEntries(const Value<>& val) { getMember(sequentialDataRef, SequentialData, numberOfEntries).store(val); }

void SequentialDataRef::setNumberOfTotalEntries(const Value<>& val) { getMember(sequentialDataRef, SequentialData, totalNumberOfEntries).store(val); }

SequentialDataRefIter SequentialDataRef::begin() { return {*this}; }

SequentialDataRefIter SequentialDataRef::at(Value<UInt64> pos) {
    SequentialDataRefIter sequentialDataRefIter(*this);
    sequentialDataRefIter.setPos(pos);
    return sequentialDataRefIter;
}

SequentialDataRefIter SequentialDataRef::end() { return at(this->getTotalNumberOfEntries()); }

SequentialDataRefIter::SequentialDataRefIter(const SequentialDataRef& sequentialDataRef) : pos(0UL), sequentialDataRef(sequentialDataRef) {}

SequentialDataRefIter::SequentialDataRefIter(const SequentialDataRefIter &it) : pos(it.pos), sequentialDataRef(it.sequentialDataRef) {}

SequentialDataRefIter& SequentialDataRefIter::operator=(const SequentialDataRefIter &it) {
    if (this == &it) {
        return *this;
    }

    pos = it.pos;
    return *this;
}

Value<MemRef> SequentialDataRefIter::operator*() {
    return sequentialDataRef.getEntry(pos);
}

SequentialDataRefIter& SequentialDataRefIter::operator++() {
    pos = pos + 1;
    return *this;
}

SequentialDataRefIter SequentialDataRefIter::operator++(int) {
    SequentialDataRefIter copy = *this;
    pos = pos + 1;
    return copy;
}

bool SequentialDataRefIter::operator==(const SequentialDataRefIter &other) const {
    if (this == &other) {
        return true;
    }

    return pos == other.pos;
}

bool SequentialDataRefIter::operator!=(const SequentialDataRefIter &other) const {
    return !(*this == other);
}

void SequentialDataRefIter::setPos(Value<UInt64> newValue) {
    pos = newValue;
}
}// namespace NES::Nautilus::Interface