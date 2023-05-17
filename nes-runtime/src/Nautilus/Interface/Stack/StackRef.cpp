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
#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Nautilus/Interface/Stack/ListRef.hpp>

namespace NES::Nautilus::Interface {
ListRef::ListRef(const Value<MemRef>& stackRef, uint64_t entrySize)
    : stackRef(stackRef), entrySize(entrySize), entriesPerPage(Stack::PAGE_SIZE / entrySize) {}

void allocateNewPageProxy(void* stackPtr) {
    auto* stack = (Stack*) stackPtr;
    stack->appendPage();
}

void* getStackPageProxy(void* stackPtr, uint64_t pagePos) {
    auto* stack = (Stack*) stackPtr;
    return stack->getPages()[pagePos];
}

Value<MemRef> ListRef::allocateEntry() {
    // check if we should allocate a new page
    if (getNumberOfEntries() >= entriesPerPage) {
        FunctionCall("allocateNewPageProxy", allocateNewPageProxy, stackRef);
    }
    // gets the current page and reserve space for the new entry.
    auto page = getCurrentPage();
    auto entry = page + (getNumberOfEntries() * entrySize);
    setNumberOfEntries(getNumberOfEntries() + (uint64_t) 1);
    setNumberOfTotalEntries(getTotalNumberOfEntries() + (uint64_t) 1);
    return entry.as<MemRef>();
}

Value<MemRef> ListRef::getCurrentPage() { return getMember(stackRef, Stack, currentPage).load<MemRef>(); }

Value<UInt64> ListRef::getNumberOfEntries() { return getMember(stackRef, Stack, numberOfEntries).load<UInt64>(); }

Value<UInt64> ListRef::getTotalNumberOfEntries() { return getMember(stackRef, Stack, totalNumberOfEntries).load<UInt64>(); }

Value<MemRef> ListRef::getPage(const Value<>& pos) {
    return (getMember(stackRef, Stack, firstPage).load<MemRef>() + (pos * 8)).as<MemRef>();
}

Value<MemRef> ListRef::getEntry(const Value<UInt64>& pos) {
    auto pagePos = (pos / entriesPerPage).as<UInt64>();
    auto positionOnPage = pos - (pagePos * entriesPerPage);

    // TODO change this later to
    auto page = Nautilus::FunctionCall("getStackPageProxy", getStackPageProxy, stackRef, Value<UInt64>(pagePos));
    auto ptrOnPage = (positionOnPage * entrySize);
    auto retPos = page + ptrOnPage;
    return retPos.as<MemRef>();
}

void ListRef::setNumberOfEntries(const Value<>& val) { getMember(stackRef, Stack, numberOfEntries).store(val); }

void ListRef::setNumberOfTotalEntries(const Value<>& val) { getMember(stackRef, Stack, totalNumberOfEntries).store(val); }


ListRefIter ListRef::begin() {
    return {*this};
}

ListRefIter ListRef::at(Value<UInt64> pos) {
    ListRefIter stackRefIter(*this);
    stackRefIter.setPos(pos);
    return stackRefIter;
}

ListRefIter ListRef::end() {
    return at(this->getTotalNumberOfEntries());
}

ListRefIter::ListRefIter(const ListRef& stackRef) : pos(0UL), stackRef(stackRef) {}

ListRefIter::ListRefIter(const ListRefIter &it) : pos(it.pos), stackRef(it.stackRef) {}

ListRefIter& ListRefIter::operator=(const ListRefIter &it) {
    if (this == &it) {
        return *this;
    }

    pos = it.pos;
    return *this;
}

Value<MemRef> ListRefIter::operator*() {
    return stackRef.getEntry(pos);
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