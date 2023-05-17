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
#include <Nautilus/Interface/List/List.hpp>
#include <Nautilus/Interface/List/ListRef.hpp>

namespace NES::Nautilus::Interface {
ListRef::ListRef(const Value<MemRef>& listRef, uint64_t entrySize)
    : listRef(listRef), entrySize(entrySize), entriesPerPage(List::PAGE_SIZE / entrySize) {}

void allocateNewPageProxy(void* listPtr) {
    auto* list = (List*) listPtr;
    list->appendPage();
}

void* getListPageProxy(void* listPtr, uint64_t pagePos) {
    auto* list = (List*) listPtr;
    return list->getPages()[pagePos];
}

Value<MemRef> ListRef::allocateEntry() {
    // check if we should allocate a new page
    if (getNumberOfEntries() >= entriesPerPage) {
        FunctionCall("allocateNewPageProxy", allocateNewPageProxy, listRef);
    }
    // gets the current page and reserve space for the new entry.
    auto page = getCurrentPage();
    auto entry = page + (getNumberOfEntries() * entrySize);
    setNumberOfEntries(getNumberOfEntries() + (uint64_t) 1);
    setNumberOfTotalEntries(getTotalNumberOfEntries() + (uint64_t) 1);
    return entry.as<MemRef>();
}

Value<MemRef> ListRef::getCurrentPage() { return getMember(listRef, List, currentPage).load<MemRef>(); }

Value<UInt64> ListRef::getNumberOfEntries() { return getMember(listRef, List, numberOfEntries).load<UInt64>(); }

Value<UInt64> ListRef::getTotalNumberOfEntries() { return getMember(listRef, List, totalNumberOfEntries).load<UInt64>(); }

Value<MemRef> ListRef::getPage(const Value<>& pos) {
    return (getMember(listRef, List, firstPage).load<MemRef>() + (pos * 8)).as<MemRef>();
}

Value<MemRef> ListRef::getEntry(const Value<UInt64>& pos) {
    auto pagePos = (pos / entriesPerPage).as<UInt64>();
    auto positionOnPage = pos - (pagePos * entriesPerPage);

    // TODO change this later to
    auto page = Nautilus::FunctionCall("getListPageProxy", getListPageProxy, listRef, Value<UInt64>(pagePos));
    auto ptrOnPage = (positionOnPage * entrySize);
    auto retPos = page + ptrOnPage;
    return retPos.as<MemRef>();
}

void ListRef::setNumberOfEntries(const Value<>& val) { getMember(listRef, List, numberOfEntries).store(val); }

void ListRef::setNumberOfTotalEntries(const Value<>& val) { getMember(listRef, List, totalNumberOfEntries).store(val); }

ListRefIter ListRef::begin() { return {*this}; }

ListRefIter ListRef::at(Value<UInt64> pos) {
    ListRefIter listRefIter(*this);
    listRefIter.setPos(pos);
    return listRefIter;
}

ListRefIter ListRef::end() { return at(this->getTotalNumberOfEntries()); }

ListRefIter::ListRefIter(const ListRef& listRef) : pos(0UL), listRef(listRef) {}

ListRefIter::ListRefIter(const ListRefIter &it) : pos(it.pos), listRef(it.listRef) {}

ListRefIter& ListRefIter::operator=(const ListRefIter &it) {
    if (this == &it) {
        return *this;
    }

    pos = it.pos;
    return *this;
}

Value<MemRef> ListRefIter::operator*() {
    return listRef.getEntry(pos);
}

ListRefIter& ListRefIter::operator++() {
    pos = pos + 1;
    return *this;
}

ListRefIter ListRefIter::operator++(int) {
    ListRefIter copy = *this;
    pos = pos + 1;
    return copy;
}

bool ListRefIter::operator==(const ListRefIter &other) const {
    if (this == &other) {
        return true;
    }

    return pos == other.pos;
}

bool ListRefIter::operator!=(const ListRefIter &other) const {
    return !(*this == other);
}

void ListRefIter::setPos(Value<UInt64> newValue) {
    pos = newValue;
}
}// namespace NES::Nautilus::Interface