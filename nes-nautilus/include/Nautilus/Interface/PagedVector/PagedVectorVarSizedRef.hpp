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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZEDREF_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZEDREF_HPP_

#include <Nautilus/Interface/Record.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <API/Schema.hpp>

namespace NES::Nautilus::Interface {
class PagedVectorVarSizedRefIter;
class PagedVectorVarSizedRef {
  public:
    /**
     * @brief Constructor
     * @param pagedVectorVarSizedRef
     * @param schema
     */
    PagedVectorVarSizedRef(const Value<MemRef>& pagedVectorVarSizedRef, SchemaPtr schema);

    /**
     * @brief Writes a new record to the PagedVectorVarSizedRef
     * @param record
     */
    void writeRecord(Record record);

    /**
     * @brief Reads a record from the PagedVectorVarSizedRef
     * @param pos
     * @return Record
     */
    Record readRecord(const Value<UInt64>& pos);

    /**
     * @brief Returns the total number of entries in the PagedVectorVarSizedRef
     * @return UInt64
     */
    Value<UInt64> getTotalNumberOfEntries();

    /**
     * @brief Creates a PageVectorVarSizedRefIter that points to the first entry in the PagedVectorVarSizedRef
     * @return PagedVectorVarSizedRefIter
     */
    PagedVectorVarSizedRefIter begin();

    /**
     * @brief Creates a PageVectorVarSizedRefIter that points to the entry at the given position in the PagedVectorVarSizedRef
     * @param pos
     * @return PagedVectorVarSizedRefIter
     */
    PagedVectorVarSizedRefIter at(Value<UInt64> pos);

    /**
     * @brief Creates a PageVectorVarSizedRefIter that points to the end of the PagedVectorVarSizedRef
     * @return PagedVectorVarSizedRefIter
     */
    PagedVectorVarSizedRefIter end();

    /**
     * @brief Equality operator
     * @param other
     * @return Boolean
     */
    bool operator==(const PagedVectorVarSizedRef& other) const;

  private:
    /**
     * @brief Returns the capacity per page
     * @return UInt64
     */
    Value<UInt64> getCapacityPerPage();

    /**
     * @brief Sets the total number of entries to the given value
     * @param val
     */
    void setTotalNumberOfEntries(const Value<>& val);

    /**
     * @brief Returns the number of entries on the current page
     * @return UInt64
     */
    Value<UInt64> getNumberOfEntriesOnCurrPage();

    /**
     * @brief Sets the number of entries on the current page
     * @param val
     */
    void setNumberOfEntriesOnCurrPage(const Value<>& val);

    Value<MemRef> pagedVectorVarSizedRef;
    const SchemaPtr schema;
};

class PagedVectorVarSizedRefIter {
  public:
    friend class PagedVectorVarSizedRef;

    /**
     * @brief Constructor
     * @param pagedVectorVarSized
     */
    explicit PagedVectorVarSizedRefIter(const PagedVectorVarSizedRef& pagedVectorVarSized);

    /**
     * @brief Dereference operator that returns the record at a given entry in the ListRef
     * @return Record
     */
    Record operator*();

    /**
     * @brief Pre-increment operator that first increments and then returns the reference
     * @return Reference
     */
    PagedVectorVarSizedRefIter& operator++();

    /**
     * @brief Equality operator
     * @param other
     * @return Boolean
     */
    bool operator==(const PagedVectorVarSizedRefIter& other) const;

    /**
     * @brief Inequality operator
     * @param other
     * @return Boolean
     */
    bool operator!=(const PagedVectorVarSizedRefIter& other) const;

  private:
    /**
     * @brief Sets the position with the newValue
     * @param newValue
     */
    void setPos(Value<UInt64> newValue);

    Value<UInt64> pos;
    PagedVectorVarSizedRef pagedVectorVarSized;
};

} //namespace NES::Nautilus::Interface

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZEDREF_HPP_
