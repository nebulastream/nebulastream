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
#include <Nautilus/Interface/FixedPage/FixedPageRef.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Interface {
PagedVectorRef::PagedVectorRef(const Value<MemRef>& pagedVectorRef, uint64_t entrySize)
    : pagedVectorRef(pagedVectorRef), entrySize(entrySize) {}

void* allocateNewPageProxy(void* pagedVectorPtr) {
    auto* pagedVector = (PagedVector*) pagedVectorPtr;
    return pagedVector->appendPage();
}

void* getEntryProxy(void* pagedVectorPtr, uint64_t pos) {
    auto* pagedVector = (PagedVector*) pagedVectorPtr;
    return pagedVector->getEntry(pos);
}

void* getFixedPageProxy(void* pagedVectorPtr, uint64_t pageNo) {
    auto* pagedVector = (PagedVector*) pagedVectorPtr;
    return pagedVector->getFixedPage(pageNo);
}

uint64_t getNumOfPagesProxy(void* pagedVectorPtr) {
    auto* pagedVector = (PagedVector*) pagedVectorPtr;
    return pagedVector->getNumberOfPages();
}

uint64_t getPageNoProxy(void* pagedVectorPtr, uint64_t pos) {
    auto* pagedVector = (PagedVector*) pagedVectorPtr;
    return pagedVector->getPageNo(pos);
}

Value<UInt64> PagedVectorRef::getCapacityPerPage() const {
    return getMember(pagedVectorRef, PagedVector, capacityPerPage).load<UInt64>();
}

Value<MemRef> PagedVectorRef::allocateEntry(const Value<UInt64>& hash) {
    // get the current FixedPage and currentPos
    auto page = getCurrentPage();
    auto pos = getCurrentPosOnPage(page);

    // reserve space for the new entry
    auto entry = getDataPtrOfPage(page) + (pos * entrySize);

    // check if we should allocate a new page and update entry pointer, page pointer and pos if needed
    if (pos >= getCapacityPerPage()) {
        entry = FunctionCall("allocateNewPageProxy", allocateNewPageProxy, pagedVectorRef);
        page = getCurrentPage();
        pos = 0_u64;
    }

    //TODO replace FunctionCall with Nautilus alternative, see #4176
    FunctionCall("addHashToBloomFilterProxy", addHashToBloomFilterProxy, page, hash);

    // update currentPos and totalNumberOfEntries
    setCurrentPosOnPage(page, pos + 1_u64);
    setNumberOfTotalEntries(getTotalNumberOfEntries() + 1_u64);

    return entry.as<MemRef>();
}

Value<MemRef> PagedVectorRef::getDataPtrOfPage(const Value<NES::Nautilus::MemRef>& fixedPageRef) {
    return getMember(fixedPageRef, FixedPage, data).load<MemRef>();
}

Value<MemRef> PagedVectorRef::getCurrentPage() {
    return getMember(pagedVectorRef, PagedVector, currentPage).load<MemRef>();
}

Value<UInt64> PagedVectorRef::getCurrentPosOnPage(const Value<MemRef>& fixedPageRef) {
    return getMember(fixedPageRef, FixedPage, currentPos).load<UInt64>();
}

Value<UInt64> PagedVectorRef::getTotalNumberOfEntries() {
    return getMember(pagedVectorRef, PagedVector, totalNumberOfEntries).load<UInt64>();
}

Value<MemRef> PagedVectorRef::getEntry(const Value<UInt64>& pos) {
    return Nautilus::FunctionCall("getEntryProxy", getEntryProxy, pagedVectorRef, pos).as<MemRef>();
}

void PagedVectorRef::setCurrentPosOnPage(const Value<MemRef>& fixedPageRef, const Value<>& val) {
    getMember(fixedPageRef, FixedPage, currentPos).store(val);
}

void PagedVectorRef::setNumberOfTotalEntries(const Value<>& val) {
    getMember(pagedVectorRef, PagedVector, totalNumberOfEntries).store(val);
}

Value<UInt64> PagedVectorRef::getEntrySize() const { return entrySize; }

Value<MemRef> PagedVectorRef::getFixedPage(const Value<NES::Nautilus::UInt64>& pageNo) {
    return Nautilus::FunctionCall("getFixedPageProxy", getFixedPageProxy, pagedVectorRef, pageNo).as<MemRef>();
}

Value<UInt64> PagedVectorRef::getNumOfPages() {
    return Nautilus::FunctionCall("getNumOfPagesProxy", getNumOfPagesProxy, pagedVectorRef).as<UInt64>();
}

Value<UInt64> PagedVectorRef::getPageNo(const Value<UInt64>& pos) {
    return Nautilus::FunctionCall("getPageNoProxy", getPageNoProxy, pagedVectorRef, pos).as<UInt64>();
}

PagedVectorRefIter PagedVectorRef::begin() {
    return at(0_u64);
}

PagedVectorRefIter PagedVectorRef::at(const Value<UInt64>& pos) {
    auto pageNo = getPageNo(pos);
    auto pageRef = getFixedPage(pageNo);
    auto dataPtrOfPage = getDataPtrOfPage(pageRef);

    PagedVectorRefIter pagedVectorRefIter(*this);
    pagedVectorRefIter.pageNo = pageNo;
    pagedVectorRefIter.fixedPageRef = pageRef;
    pagedVectorRefIter.lastAddrOnPage = dataPtrOfPage + (getCurrentPosOnPage(pageRef) * entrySize);
    pagedVectorRefIter.addr = getEntry(pos);
    return pagedVectorRefIter;
}

PagedVectorRefIter PagedVectorRef::end() {
    PagedVectorRefIter pagedVectorRefIter(*this);
    auto page = getCurrentPage();
    pagedVectorRefIter.fixedPageRef = page;
    pagedVectorRefIter.pageNo = getNumOfPages() - 1_u64;
    pagedVectorRefIter.lastAddrOnPage = getDataPtrOfPage(page) + (getCurrentPosOnPage(page) * entrySize);
    pagedVectorRefIter.addr = pagedVectorRefIter.lastAddrOnPage;

    return pagedVectorRefIter;
}

bool PagedVectorRef::operator==(const PagedVectorRef& other) const {
    if (this == &other) {
        return true;
    }

    return entrySize == other.entrySize && pagedVectorRef == other.pagedVectorRef;
}

PagedVectorRefIter::PagedVectorRefIter(PagedVectorRef& pagedVectorRef)
    : pagedVectorRef(pagedVectorRef), fixedPageRef(pagedVectorRef.getFixedPage(0_u64)), pageNo(0_u64),
      lastAddrOnPage(getLastAddrOnPage()), addr(pagedVectorRef.getDataPtrOfPage(fixedPageRef)) {}

PagedVectorRefIter::PagedVectorRefIter(const PagedVectorRefIter& it)
    : pagedVectorRef(it.pagedVectorRef), fixedPageRef(it.fixedPageRef), pageNo(it.pageNo), lastAddrOnPage(it.lastAddrOnPage),
      addr(it.addr) {}

PagedVectorRefIter& PagedVectorRefIter::operator=(const PagedVectorRefIter& it) {
    if (this == &it) {
        return *this;
    }

    fixedPageRef = it.fixedPageRef;
    pageNo = it.pageNo;
    lastAddrOnPage = it.lastAddrOnPage;
    addr = it.addr;
    pagedVectorRef = it.pagedVectorRef;
    return *this;
}

Value<MemRef> PagedVectorRefIter::operator*() { return addr; }

PagedVectorRefIter& PagedVectorRefIter::operator++() {
    incrementAddress();
    return *this;
}

PagedVectorRefIter PagedVectorRefIter::operator++(int) {
    PagedVectorRefIter copy = *this;
    incrementAddress();
    return copy;
}

bool PagedVectorRefIter::operator==(const PagedVectorRefIter& other) const {
    if (this == &other) {
        return true;
    }

    auto sameFixedPageRef = (fixedPageRef == other.fixedPageRef);
    auto samePageNumber = (pageNo == other.pageNo);
    auto sameLastAddress = (lastAddrOnPage == other.lastAddrOnPage);
    auto sameAddress = (addr == other.addr);
    auto samePagedVectorRef = (pagedVectorRef == other.pagedVectorRef);
    return sameFixedPageRef && samePageNumber && sameLastAddress && sameAddress && samePagedVectorRef;
}

bool PagedVectorRefIter::operator!=(const PagedVectorRefIter& other) const { return !(*this == other); }

void PagedVectorRefIter::incrementAddress() {
    addr = addr + pagedVectorRef.getEntrySize();
    if (addr == lastAddrOnPage && pageNo != pagedVectorRef.getNumOfPages() - 1_u64) {
        pageNo = pageNo + 1_u64;
        fixedPageRef = pagedVectorRef.getFixedPage(pageNo);
        lastAddrOnPage = getLastAddrOnPage();
        addr = pagedVectorRef.getDataPtrOfPage(fixedPageRef);
    }
}

Value<MemRef> PagedVectorRefIter::getLastAddrOnPage() {
    auto pageDataPtr = pagedVectorRef.getDataPtrOfPage(fixedPageRef);
    auto lastAddr = pageDataPtr + (pagedVectorRef.getCurrentPosOnPage(fixedPageRef) * pagedVectorRef.getEntrySize());
    return lastAddr.as<MemRef>();
}
}// namespace NES::Nautilus::Interface