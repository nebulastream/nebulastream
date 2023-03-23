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
