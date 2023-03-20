#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACKREF_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACKREF_HPP_
#include <Nautilus/Interface/DataTypes/Value.hpp>
namespace NES::Nautilus::Interface {
class StackRef {
  public:
    StackRef(const Value<MemRef>& stackRef, uint64_t entrySize);
    Value<MemRef> allocateEntry();
    Value<UInt64> getNumberOfEntries();
    void setNumberOfEntries(Value<> val);
  private:
    Value<MemRef> getCurrentPage();
    Value<MemRef> stackRef;
    uint64_t entrySize;
    uint64_t entriesPerPage;
};
}// namespace NES::Nautilus::Interface

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACKREF_HPP_
