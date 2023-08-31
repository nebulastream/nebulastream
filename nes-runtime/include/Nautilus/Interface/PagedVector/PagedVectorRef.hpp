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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORREF_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORREF_HPP_
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
namespace NES::Nautilus::Interface {

// Forward declaration
class PagedVectorRefIter;

/**
 * @brief This is a nautilus wrapper for the sequential data structure.
 * It wraps a memref to the underling sequential and provides access methods.
 */
class PagedVectorRef {
  public:
    /**
     * @brief Constructs the wrapper.
     * @param pagedVectorRef memref to the list
     * @param entrySize size of entries.
     */
    PagedVectorRef(const Value<MemRef>& pagedVectorRef, uint64_t entrySize);

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
     * @brief Returns the total number of entries for this list.
     * @return Value<UInt64>
     */
    Value<UInt64> getTotalNumberOfEntries();

    /**
     * @brief Returns the maximum number of records per page
     * @return Value<UInt64>
     */
    Value<UInt64> getCapacityPerPage() const;

    /**
     * @brief Returns an iterator that points to the begin of this listRef
     * @return ListRefIter
     */
    PagedVectorRefIter begin();

    /**
     * @brief Returns an iterator pointing to the entry at pos
     * @param pos
     * @return ListRefIter
     */
    PagedVectorRefIter at(Value<UInt64> pos);

    /**
     * @brief Returns an iterator that points to the end of this ListRef
     * @return ListRefIter
     */
    PagedVectorRefIter end();

    /**
     * @brief Equality operator
     * @param other
     * @return Boolean
     */
    bool operator==(const PagedVectorRef& other) const;

  private:
    Value<MemRef> getCurrentPage();
    Value<MemRef> pagedVectorRef;
    uint64_t entrySize;
};

class PagedVectorRefIter {
  public:
    friend class PagedVectorRef;

    /**
     * @brief Constructor
     * @param listRef
     */
    PagedVectorRefIter(const PagedVectorRef& listRef);

    /**
     * @brief Copy constructor
     * @param it
     */
    PagedVectorRefIter(const PagedVectorRefIter& it);

    /**
     * @brief Assignment operator
     * @param it
     * @return Reference to ListRefIter
     */
    PagedVectorRefIter& operator=(const PagedVectorRefIter& it);

    /**
     * @brief Dereference operator that points to a given entry in the ListRef
     * @return Value<MemRef>
     */
    Value<MemRef> operator*();

    /**
     * @brief Pre-increment operator that first increments and then returns the reference
     * @return Reference
     */
    PagedVectorRefIter& operator++();

    /**
     * @brief Post-increment count that first returns the reference and then increments
     * @return Iterator
     */
    PagedVectorRefIter operator++(int);

    /**
     * @brief Equality operator
     * @param other
     * @return Boolean
     */
    bool operator==(const PagedVectorRefIter& other) const;

    /**
     * @brief Inequality operator
     * @param other
     * @return Boolean
     */
    bool operator!=(const PagedVectorRefIter& other) const;

  private:
    /**
     * @brief Sets the position with the newValue
     * @param newValue
     */
    void setPos(Value<UInt64> newValue);

    Value<UInt64> pos;
    PagedVectorRef pagedVectorRef;
};

}// namespace NES::Nautilus::Interface

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORREF_HPP_
