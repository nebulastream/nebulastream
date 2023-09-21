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
#include <Util/StdInt.hpp>

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
     * @brief Allocates an new entry and returns a reference to it. This method is not thread safe.
     * @param hash
     * @return Value<MemRef>
     */
    Value<MemRef> allocateEntry(const Value<UInt64>& hash = 0_u64);

    /**
     * @brief Returns the reference to the start of the record at the pos
     * @param pos
     * @return Value<MemRef>
     */
    Value<MemRef> getEntry(const Value<UInt64>& pos);

    /**
     * @brief Returns the currentPos on the current page.
     * @param fixedPageRef
     * @return Value<UInt64>
     */
    Value<UInt64> getCurrentPosOnPage(const Value<MemRef>& fixedPageRef);

    /**
     * @brief Modifies the currentPos on the current page.
     * @param fixedPageRef
     * @param entries
     */
    void setCurrentPosOnPage(const Value<MemRef>& fixedPageRef, const Value<>& entries);

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
     * @brief Returns a pointer to the data of the FixedPage
     * @param fixedPagePtr
     * @return Value<MemRef>
     */
    Value<MemRef> getDataPtrOfPage(const Value<MemRef>& fixedPageRef);

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
    PagedVectorRefIter at(const Value<UInt64>& pos);

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

    /**
     * @brief Returns the currentPage.
     * @return Value<MemRef>
     */
    Value<MemRef> getCurrentPage();

    /**
     * @brief Returns the page with given page number.
     * @return Value<MemRef>
     */
    Value<MemRef> getFixedPage(const Value<UInt64>& pageNo);

    /**
     * @brief Returns the number of pages.
     * @return Value<UInt64>
     */
    Value<UInt64> getNumOfPages();

    /**
     * @brief Returns the number of the page of the record at pos
     * @param pos
     * @return Value<UInt64>
     */
    Value<UInt64> getPageNo(const Value<UInt64>& pos);

    /**
     * @brief Getter for entrySize
     * @return Value<UInt64>
     */
    Value<UInt64> getEntrySize() const;

  private:
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
    PagedVectorRefIter(PagedVectorRef& listRef);

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
     * @brief increments address and if necessary updates fixedPageRef and pageNo
     */
    void incrementAddress();

    /**
     * @brief Returns the last address on the current fixedPageRef
     * @return Value<MemRef>
     */
    Value<MemRef> getLastAddrOnPage();

    PagedVectorRef pagedVectorRef;
    Value<MemRef> fixedPageRef;
    Value<UInt64> pageNo;
    Value<MemRef> lastAddrOnPage;
    Value<MemRef> addr;
};

}// namespace NES::Nautilus::Interface

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORREF_HPP_
