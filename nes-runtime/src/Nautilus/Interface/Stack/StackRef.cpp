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