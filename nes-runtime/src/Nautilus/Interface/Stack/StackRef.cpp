#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/Stack/StackRef.hpp>
#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Nautilus::Interface {

StackRef::StackRef(const Value<MemRef>& stackRef, uint64_t entrySize) : stackRef(stackRef), entrySize(entrySize) {}

void allocateNewChunkProxy(void* stackPtr){
    auto* stack = (Stack*) stackPtr;
    stack->createChunk();
}

Value<MemRef> StackRef::allocateEntry() {
    if(numberOfEntries() > 10){
        FunctionCall("allocateNewChunkProxy", allocateNewChunkProxy, stackRef);
    }
    auto page = getCurrentPage();
    auto entry = page + (numberOfEntries() * entrySize);
    return entry.as<MemRef>();
}

Value<MemRef> StackRef::getCurrentPage() {
    return getMember(stackRef, Stack, currentPage).load<MemRef>();
}

Value<UInt64> StackRef::numberOfEntries() {
    return getMember(stackRef, Stack, numberOfEntries).load<UInt64>();
}

}// namespace NES::Nautilus::Interface