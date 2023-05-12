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

void* getEntryProxy(void* stackPtr, uint64_t pos) {
    auto* stack = (Stack*) stackPtr;
    return stack->getEntry(pos);
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

Value<UInt64> StackRef::getTotalNumberOfEntries() {
    return getMember(stackRef, Stack, totalNumberOfEntries).load<UInt64>();
}

Value<MemRef> StackRef::getPage(const Value<>& pos) {
    return (getMember(stackRef, Stack, pages).load<MemRef>() + pos).as<MemRef>();
}

Value<MemRef> StackRef::getEntry(const Value<UInt64>& pos) {
//    return FunctionCall("getEntryProxy", getEntryProxy, stackRef, pos);
    auto pagePos = pos / entriesPerPage;

    // As I can not find a modulo operator, I have to do this weird loop
    auto positionOnPage = pos;
    while(positionOnPage >= entriesPerPage) {
        positionOnPage = positionOnPage - entriesPerPage;
    }
    return (getPage(pos) + (positionOnPage * entrySize)).as<MemRef>();
}

void StackRef::setNumberOfEntries(const Value<>& val) { getMember(stackRef, Stack, numberOfEntries).store(val); }

void StackRef::setNumberOfTotalEntries(const Value<>& val) { getMember(stackRef, Stack, totalNumberOfEntries).store(val); }


}// namespace NES::Nautilus::Interface