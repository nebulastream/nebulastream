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
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>

namespace NES::Nautilus::Interface {
PagedVectorRef::PagedVectorRef(const Value<MemRef>& pagedVectorRef, uint64_t entrySize)
    : pagedVectorRef(pagedVectorRef), entrySize(entrySize) {}

void allocateNewPageProxy(void* pagedVectorPtr) {
    auto* pagedVector = (PagedVector*) pagedVectorPtr;
    pagedVector->appendPage();
}

void* getPagedVectorPageProxy(void* pagedVectorPtr, uint64_t pagePos) {
    auto* pagedVector = (PagedVector*) pagedVectorPtr;
    return pagedVector->getPages()[pagePos];
}

Value<UInt64> PagedVectorRef::getCapacityPerPage() const {
    return getMember(pagedVectorRef, PagedVector, capacityPerPage).load<UInt64>();
}

Value<MemRef> PagedVectorRef::allocateEntry() {
    // check if we should allocate a new page
    if (getNumberOfEntries() >= getCapacityPerPage()) {
        FunctionCall("allocateNewPageProxy", allocateNewPageProxy, pagedVectorRef);
    }
    // gets the current page and reserve space for the new entry.
    auto page = getCurrentPage();
    auto entry = page + (getNumberOfEntries() * entrySize);
    setNumberOfEntries(getNumberOfEntries() + 1_u64);
    setNumberOfTotalEntries(getTotalNumberOfEntries() + 1_u64);

    return entry.as<MemRef>();
}

Value<MemRef> PagedVectorRef::getCurrentPage() { return getMember(pagedVectorRef, PagedVector, currentPage).load<MemRef>(); }

Value<UInt64> PagedVectorRef::getNumberOfEntries() {
    return getMember(pagedVectorRef, PagedVector, numberOfEntries).load<UInt64>();
}

Value<UInt64> PagedVectorRef::getTotalNumberOfEntries() {
    return getMember(pagedVectorRef, PagedVector, totalNumberOfEntries).load<UInt64>();
}

Value<MemRef> PagedVectorRef::getEntry(const Value<UInt64>& pos) {
    // Calculating on what page and at what position the entry lies
    Value<UInt64> capacityPerPage = getCapacityPerPage();
    auto pagePos = (pos / capacityPerPage).as<UInt64>();
    auto positionOnPage = pos - (pagePos * capacityPerPage);

    auto page =
        Nautilus::FunctionCall("getPagedVectorPageProxy", getPagedVectorPageProxy, pagedVectorRef, Value<UInt64>(pagePos));
    auto ptrOnPage = (positionOnPage * entrySize);
    auto retPos = page + ptrOnPage;
    return retPos.as<MemRef>();
}

void PagedVectorRef::setNumberOfEntries(const Value<>& val) {
    getMember(pagedVectorRef, PagedVector, numberOfEntries).store(val);
}

void PagedVectorRef::setNumberOfTotalEntries(const Value<>& val) {
    getMember(pagedVectorRef, PagedVector, totalNumberOfEntries).store(val);
}

PagedVectorRefIter PagedVectorRef::begin() { return at(0_u64); }

PagedVectorRefIter PagedVectorRef::at(Value<UInt64> pos) {
    PagedVectorRefIter pagedVectorRefIter(*this);
    pagedVectorRefIter.setPos(pos);
    return pagedVectorRefIter;
}

PagedVectorRefIter PagedVectorRef::end() { return at(this->getTotalNumberOfEntries()); }

PagedVectorRefIter::PagedVectorRefIter(const PagedVectorRef& pagedVectorRef) : pos(0_u64), pagedVectorRef(pagedVectorRef) {}

PagedVectorRefIter::PagedVectorRefIter(const PagedVectorRefIter& it) : pos(it.pos), pagedVectorRef(it.pagedVectorRef) {}

PagedVectorRefIter& PagedVectorRefIter::operator=(const PagedVectorRefIter& it) {
    if (this == &it) {
        return *this;
    }

    pos = it.pos;
    return *this;
}

Value<MemRef> PagedVectorRefIter::operator*() { return pagedVectorRef.getEntry(pos); }

PagedVectorRefIter& PagedVectorRefIter::operator++() {
    pos = pos + 1;
    return *this;
}

PagedVectorRefIter PagedVectorRefIter::operator++(int) {
    PagedVectorRefIter copy = *this;
    pos = pos + 1;
    return copy;
}

bool PagedVectorRefIter::operator==(const PagedVectorRefIter& other) const {
    if (this == &other) {
        return true;
    }

    return pos == other.pos;
}

bool PagedVectorRefIter::operator!=(const PagedVectorRefIter& other) const { return !(*this == other); }

void PagedVectorRefIter::setPos(Value<UInt64> newValue) { pos = newValue; }
}// namespace NES::Nautilus::Interface