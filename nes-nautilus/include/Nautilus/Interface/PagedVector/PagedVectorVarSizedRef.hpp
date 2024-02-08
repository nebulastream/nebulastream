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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZEDREF_H
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZEDREF_H

#include <Nautilus/Interface/Record.hpp>
#include <API/Schema.hpp>

namespace NES::Nautilus::Interface {
class PagedVectorVarSizedRefIter;
class PagedVectorVarSizedRef {
  public:
    PagedVectorVarSizedRef(const Value<MemRef>& pagedVectorVarSizedRef, SchemaPtr schema);

    void writeRecord(Record record);

    Record readRecord(const Value<UInt64>& pos);

    Value<UInt64> getTotalNumberOfEntries();

    PagedVectorVarSizedRefIter begin();

    PagedVectorVarSizedRefIter at(Value<UInt64> pos);

    PagedVectorVarSizedRefIter end();

    /**
     * @brief Equality operator
     * @param other
     * @return Boolean
     */
    bool operator==(const PagedVectorVarSizedRef& other) const;

  private:
    Value<MemRef> getCurrentPage();
    Value<UInt64> getCapacityPerPage();
    Value<MemRef> getCurrentVarSizedDataEntry();
    Value<UInt64> getEntrySize();
    void setTotalNumberOfEntries(const Value<>& val);

    Value<MemRef> pagedVectorVarSizedRef;
    const SchemaPtr schema;
};

class PagedVectorVarSizedRefIter {
  public:
    friend class PagedVectorVarSizedRef;

    /**
     * @brief Constructor
     * @param listRef
     */
    PagedVectorVarSizedRefIter(const PagedVectorVarSizedRef& listRef);

    /**
     * @brief Copy constructor
     * @param it
     */
    PagedVectorVarSizedRefIter(const PagedVectorVarSizedRefIter& it);

    /**
     * @brief Assignment operator
     * @param it
     * @return Reference to ListRefIter
     */
    PagedVectorVarSizedRefIter& operator=(const PagedVectorVarSizedRefIter& it);

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
     * @brief Post-increment count that first returns the reference and then increments
     * @return Iterator
     */
    PagedVectorVarSizedRefIter operator++(int);

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

#endif//NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZEDREF_H
