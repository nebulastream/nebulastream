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

#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Nautilus::Interface {
PagedVectorRef::PagedVectorRef(const Nautilus::MemRefVal& pagedVectorRef, uint64_t entrySize)
    : pagedVectorRef(pagedVectorRef), entrySize(entrySize) {}

void allocateNewPageProxy(void* pagedVectorPtr) {
    auto* pagedVector = (PagedVector*) pagedVectorPtr;
    pagedVector->appendPage();
}

int8_t* getPagedVectorPageProxy(void* pagedVectorPtr, uint64_t pagePos) {
    auto* pagedVector = (PagedVector*) pagedVectorPtr;
    return pagedVector->getPages()[pagePos];
}

Nautilus::UInt64Val PagedVectorRef::getCapacityPerPage() const {
    return getMember(pagedVectorRef, PagedVector, capacityPerPage, uint64_t);
}

Nautilus::MemRefVal PagedVectorRef::allocateEntry() {
    // check if we should allocate a new page
    const auto capacityPerPage = getCapacityPerPage();
    const auto numberOfEntries = getNumberOfEntries();
    if (numberOfEntries >= capacityPerPage) {
        nautilus::invoke(allocateNewPageProxy, pagedVectorRef);
    }
    // gets the current page and reserve space for the new entry.
    auto page = getCurrentPage();
    auto entry = page + (getNumberOfEntries() * entrySize);
    setNumberOfEntries(getNumberOfEntries() + 1_u64);
    setNumberOfTotalEntries(getTotalNumberOfEntries() + 1_u64);

    return entry;
}

Nautilus::MemRefVal PagedVectorRef::getCurrentPage() { return getMemberAsPointer(pagedVectorRef, PagedVector, currentPage, int8_t); }

Nautilus::UInt64Val PagedVectorRef::getNumberOfEntries() {
    return getMember(pagedVectorRef, PagedVector, numberOfEntries, uint64_t);
}

Nautilus::UInt64Val PagedVectorRef::getTotalNumberOfEntries() {
    return getMember(pagedVectorRef, PagedVector, totalNumberOfEntries, uint64_t);
}

Nautilus::MemRefVal PagedVectorRef::getEntry(const Nautilus::UInt64Val& pos) {
    // Calculating on what page and at what position the entry lies
    Nautilus::UInt64Val capacityPerPage = getCapacityPerPage();
    auto pagePos = (pos / capacityPerPage);
    auto positionOnPage = pos - (pagePos * capacityPerPage);

    auto page = nautilus::invoke(getPagedVectorPageProxy, pagedVectorRef, Nautilus::UInt64Val(pagePos));
    auto ptrOnPage = (positionOnPage * entrySize);
    auto retPos = page + ptrOnPage;
    return retPos;
}

void PagedVectorRef::setNumberOfEntries(const UInt64Val& val) {
    *getMemberAsPointer(pagedVectorRef, PagedVector, numberOfEntries, uint64_t) = val;
}

void PagedVectorRef::setNumberOfTotalEntries(const UInt64Val& val) {
    *getMemberAsPointer(pagedVectorRef, PagedVector, totalNumberOfEntries, uint64_t) = val;
}

PagedVectorRefIter PagedVectorRef::begin() { return at(0_u64); }

PagedVectorRefIter PagedVectorRef::at(Nautilus::UInt64Val pos) {
    PagedVectorRefIter pagedVectorRefIter(*this);
    pagedVectorRefIter.setPos(pos);
    return pagedVectorRefIter;
}

PagedVectorRefIter PagedVectorRef::end() { return at(this->getTotalNumberOfEntries()); }

bool PagedVectorRef::operator==(const PagedVectorRef& other) const {
    if (this == &other) {
        return true;
    }

    return entrySize == other.entrySize && pagedVectorRef == other.pagedVectorRef;
}

PagedVectorRefIter::PagedVectorRefIter(const PagedVectorRef& pagedVectorRef) : pos(0_u64), pagedVectorRef(pagedVectorRef) {}

PagedVectorRefIter::PagedVectorRefIter(const PagedVectorRefIter& it) : pos(it.pos), pagedVectorRef(it.pagedVectorRef) {}

PagedVectorRefIter& PagedVectorRefIter::operator=(const PagedVectorRefIter& it) {
    if (this == &it) {
        return *this;
    }

    pos = it.pos;
    pagedVectorRef = it.pagedVectorRef;
    return *this;
}

Nautilus::MemRefVal PagedVectorRefIter::operator*() { return pagedVectorRef.getEntry(pos); }

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

    return pos == other.pos && pagedVectorRef == other.pagedVectorRef;
}

bool PagedVectorRefIter::operator!=(const PagedVectorRefIter& other) const { return !(*this == other); }

void PagedVectorRefIter::setPos(Nautilus::UInt64Val newValue) { pos = newValue; }
}// namespace NES::Nautilus::Interface
