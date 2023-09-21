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
#include <Nautilus/Interface/FixedPage/FixedPage.hpp>
#include <Nautilus/Interface/FixedPage/FixedPageRef.hpp>
#include <Util/Logger/Logger.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Nautilus::Interface {

void addHashToBloomFilterProxy(void* fixedPagePtr, uint64_t hash) {
    auto* fixedPage = (FixedPage*) fixedPagePtr;
    fixedPage->addHashToBloomFilter(hash);
}

FixedPageRef::FixedPageRef(const Value<MemRef>& fixedPageRef)
    : fixedPageRef(fixedPageRef) {}

Value<MemRef> FixedPageRef::allocateEntry(const Value<UInt64>& hash) {
    auto currentPos = getCurrentPos();

    Value<MemRef> entry(nullptr);
    if (currentPos < getCapacity()) {
        //TODO replace FunctionCall with Nautilus alternative, see #4176
        FunctionCall("addHashToBloomFilterProxy", addHashToBloomFilterProxy, fixedPageRef, hash);

        auto ptr = getDataPtr() + currentPos * getSizeOfRecord();
        setCurrentPos(currentPos + 1);
        entry = ptr.as<MemRef>();
    }

    return entry;
}

Value<UInt64> FixedPageRef::getSizeOfRecord() {
    return getMember(fixedPageRef, FixedPage, sizeOfRecord).load<UInt64>();
}

Value<MemRef> FixedPageRef::getDataPtr() {
    return getMember(fixedPageRef, FixedPage, data).load<MemRef>();
}

Value<UInt64> FixedPageRef::getCurrentPos() {
    return getMember(fixedPageRef, FixedPage, currentPos).load<UInt64>();
}

Value<UInt64> FixedPageRef::getCapacity() {
    return getMember(fixedPageRef, FixedPage, capacity).load<UInt64>();
}

void FixedPageRef::setCurrentPos(const Value<>& pos) {
    getMember(fixedPageRef, FixedPage, currentPos).store(pos);
}

FixedPageRefIter FixedPageRef::begin() { return at(0_u64); }

FixedPageRefIter FixedPageRef::at(const Value<UInt64>& pos) {
    FixedPageRefIter fixedPageRefIter(*this);
    fixedPageRefIter.addr = fixedPageRefIter.addr + pos * getSizeOfRecord();
    return fixedPageRefIter;
}

FixedPageRefIter FixedPageRef::end() { return at(this->getCurrentPos()); }

bool FixedPageRef::operator==(const FixedPageRef& other) const {
    if (this == &other) {
        return true;
    }

    return fixedPageRef == other.fixedPageRef;
}

FixedPageRefIter::FixedPageRefIter(FixedPageRef& fixedPageRef)
    : addr(fixedPageRef.getDataPtr()), fixedPageRef(fixedPageRef) {}

FixedPageRefIter::FixedPageRefIter(const FixedPageRefIter& it) : addr(it.addr), fixedPageRef(it.fixedPageRef) {}

FixedPageRefIter& FixedPageRefIter::operator=(const FixedPageRefIter& it) {
    if (this == &it) {
        return *this;
    }

    addr = it.addr;
    fixedPageRef = it.fixedPageRef;
    return *this;
}

Value<MemRef> FixedPageRefIter::operator*() { return addr; }

FixedPageRefIter& FixedPageRefIter::operator++() {
    addr = addr + fixedPageRef.getSizeOfRecord();
    return *this;
}

FixedPageRefIter FixedPageRefIter::operator++(int) {
    FixedPageRefIter copy = *this;
    addr = addr + fixedPageRef.getSizeOfRecord();
    return copy;
}

bool FixedPageRefIter::operator==(const FixedPageRefIter& other) const {
    if (this == &other) {
        return true;
    }

    auto sameAddress = (addr == other.addr);
    auto sameFixedPageRef = (fixedPageRef == other.fixedPageRef);
    return sameAddress && sameFixedPageRef;
}

bool FixedPageRefIter::operator!=(const FixedPageRefIter& other) const { return !(*this == other); }
}// namespace NES::Nautilus::Interface
