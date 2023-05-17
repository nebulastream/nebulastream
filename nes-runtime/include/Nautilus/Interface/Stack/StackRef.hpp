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

// Forward declaration
class StackRefIter;

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
    Value<MemRef> getEntry(const Value<UInt64>& pos);

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
     * @brief Modifies the number of total entries
     * @param totalEntries
     */
    void setNumberOfTotalEntries(const Value<>& totalEntries);

    /**
     * @brief Returns the total number of entries for this stack.
     * @return Value<UInt64>
     */
    Value<UInt64> getTotalNumberOfEntries();

    /**
     * @brief Returns the memref to the page at position pos
     * @param pos
     * @return MemRef
     */
    Value<MemRef> getPage(const Value<>& pos);

    /**
     * @brief Returns an iterator that points to the begin of this StackRef
     * @return StackRefIter
     */
    StackRefIter begin();

    /**
     * @brief Returns an iterator pointing to the entry at pos
     * @param pos
     * @return StackRefIter
     */
    StackRefIter at(Value<UInt64> pos);

    /**
     * @brief Returns an iterator that points to the end of this StackRef
     * @return StackRefIter
     */
    StackRefIter end();


  private:
    Value<MemRef> getCurrentPage();
    Value<MemRef> stackRef;
    uint64_t entrySize;
    uint64_t entriesPerPage;
};

class StackRefIter {
public:
    friend class StackRef;

    /**
     * @brief Constructor
     * @param stackRef
     */
    StackRefIter(const StackRef& stackRef);

    /**
     * @brief Copy constructor
     * @param it
     */
    StackRefIter(const StackRefIter& it);

    /**
     * @brief Assignment operator
     * @param it
     * @return Reference to StackRefIter
     */
    StackRefIter& operator=(const StackRefIter& it);

    /**
     * @brief Dereference operator that points to a given entry in the StackRef
     * @return Value<MemRef>
     */
    Value<MemRef> operator*();

    /**
     * @brief Pre-increment operator that first increments and then returns the reference
     * @return Reference
     */
    StackRefIter& operator++();

    /**
     * @brief Post-increment count that first returns the reference and then increments
     * @return Iterator
     */
    StackRefIter operator++(int);

    /**
     * @brief Equality operator
     * @param other
     * @return Boolean
     */
    bool operator==(const StackRefIter& other) const;

    /**
     * @brief Inequality operator
     * @param other
     * @return Boolean
     */
    bool operator!=(const StackRefIter& other) const;

private:
    /**
     * @brief Sets the position with the newValue
     * @param newValue
     */
    void setPos(Value<UInt64> newValue);

    Value<UInt64> pos;
    StackRef stackRef;

};

}// namespace NES::Nautilus::Interface

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACKREF_HPP_
