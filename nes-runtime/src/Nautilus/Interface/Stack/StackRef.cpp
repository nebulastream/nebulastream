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

void allocateNewChunkProxy(void* stackPtr) {
    auto* stack = (Stack*) stackPtr;
    stack->createChunk();
}

Value<MemRef> StackRef::allocateEntry() {
    if (getNumberOfEntries() >= entriesPerPage) {
        FunctionCall("allocateNewChunkProxy", allocateNewChunkProxy, stackRef);
    }
    auto page = getCurrentPage();
    auto entry = page + (getNumberOfEntries() * entrySize);
    setNumberOfEntries(getNumberOfEntries() + (uint64_t) 1);
    return entry.as<MemRef>();
}

Value<MemRef> StackRef::getCurrentPage() { return getMember(stackRef, Stack, currentPage).load<MemRef>(); }

Value<UInt64> StackRef::getNumberOfEntries() { return getMember(stackRef, Stack, numberOfEntries).load<UInt64>(); }
void StackRef::setNumberOfEntries(Value<> val) { getMember(stackRef, Stack, numberOfEntries).store(val); }

}// namespace NES::Nautilus::Interface