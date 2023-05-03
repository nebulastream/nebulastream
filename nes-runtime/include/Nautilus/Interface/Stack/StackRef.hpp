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

/**
 * @brief This is a nautilus wrapper for the stack data structure.
 * It wraps a memref to the underling stack and provides access methods.
 */
class StackRef {
  public:
    /**
     * @brief Constructs the wrapper.
     * @param stackRef memref to the stack
     * @param entrySize size of entries.
     */
    StackRef(const Value<MemRef>& stackRef, uint64_t entrySize);

    /**
     * @brief Allocates an new entry and returns a reference to it.
     * @return Value<MemRef>
     */
    Value<MemRef> allocateEntry();

    /**
     * @brief Returns the reference to the start of the record at the pos
     * @param pos
     * @return Value<MemRef>
     */
    Value<MemRef> getEntry(Value<UInt64> pos);

    /**
     * @brief Returns the number of entries in the current page.
     * @return Value<UInt64>
     */
    Value<UInt64> getNumberOfEntries();

    /**
     * @brief Modifies the number of entries in the current page.
     * @param entries
     */
    void setNumberOfEntries(const Value<>& entries);

    /**
     * @brief Returns the total number of entries for this stack.
     * @return Value<UInt64>
     */
    Value<UInt64> getTotalNumberOfEntries();

  private:
    Value<MemRef> getCurrentPage();
    Value<MemRef> stackRef;
    uint64_t entrySize;
    uint64_t entriesPerPage;
};
}// namespace NES::Nautilus::Interface

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACKREF_HPP_
