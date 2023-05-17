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
#include <Nautilus/Interface/Stack/StackRef.hpp>

namespace NES::Nautilus::Interface {
StackRef::StackRef(const Value<MemRef>& stackRef, uint64_t entrySize)
    : stackRef(stackRef), entrySize(entrySize), entriesPerPage(Stack::PAGE_SIZE / entrySize) {}

void allocateNewPageProxy(void* stackPtr) {
    auto* stack = (Stack*) stackPtr;
    stack->appendPage();
}

void* getStackPageProxy(void* stackPtr, uint64_t pagePos) {
    auto* stack = (Stack*) stackPtr;
    return stack->getPages()[pagePos];
}

Value<MemRef> StackRef::allocateEntry() {
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

Value<MemRef> StackRef::getCurrentPage() { return getMember(stackRef, Stack, currentPage).load<MemRef>(); }

Value<UInt64> StackRef::getNumberOfEntries() { return getMember(stackRef, Stack, numberOfEntries).load<UInt64>(); }

Value<UInt64> StackRef::getTotalNumberOfEntries() { return getMember(stackRef, Stack, totalNumberOfEntries).load<UInt64>(); }

Value<MemRef> StackRef::getPage(const Value<>& pos) {
    return (getMember(stackRef, Stack, firstPage).load<MemRef>() + (pos * 8)).as<MemRef>();
}

Value<MemRef> StackRef::getEntry(const Value<UInt64>& pos) {
    auto pagePos = (pos / entriesPerPage).as<UInt64>();
    auto positionOnPage = pos - (pagePos * entriesPerPage);

    // TODO change this later to
    auto page = Nautilus::FunctionCall("getStackPageProxy", getStackPageProxy, stackRef, Value<UInt64>(pagePos));
    auto ptrOnPage = (positionOnPage * entrySize);
    auto retPos = page + ptrOnPage;
    return retPos.as<MemRef>();
}

void StackRef::setNumberOfEntries(const Value<>& val) { getMember(stackRef, Stack, numberOfEntries).store(val); }

void StackRef::setNumberOfTotalEntries(const Value<>& val) { getMember(stackRef, Stack, totalNumberOfEntries).store(val); }


StackRefIter StackRef::begin() {
    return {*this};
}

StackRefIter StackRef::at(Value<UInt64> pos) {
    StackRefIter stackRefIter(*this);
    stackRefIter.setPos(pos);
    return stackRefIter;
}

StackRefIter StackRef::end() {
    return at(this->getTotalNumberOfEntries());
}

StackRefIter::StackRefIter(const StackRef& stackRef) : pos(0UL), stackRef(stackRef) {}

StackRefIter::StackRefIter(const StackRefIter &it) : pos(it.pos), stackRef(it.stackRef) {}

StackRefIter& StackRefIter::operator=(const StackRefIter &it) {
    if (this == &it) {
        return *this;
    }

    pos = it.pos;
    return *this;
}

Value<MemRef> StackRefIter::operator*() {
    return stackRef.getEntry(pos);
}

StackRefIter& StackRefIter::operator++() {
    pos = pos + 1;
    return *this;
}

StackRefIter StackRefIter::operator++(int) {
    StackRefIter copy = *this;
    pos = pos + 1;
    return copy;
}

bool StackRefIter::operator==(const StackRefIter &other) const {
    if (this == &other) {
        return true;
    }

    return pos == other.pos;
}

bool StackRefIter::operator!=(const StackRefIter &other) const {
    return !(*this == other);
}

void StackRefIter::setPos(Value<UInt64> newValue) {
    pos = newValue;
}
}// namespace NES::Nautilus::Interface