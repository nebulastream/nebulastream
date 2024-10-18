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

#pragma once

#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>

namespace NES::Nautilus::Interface
{
class PagedVectorRefIter;

class PagedVectorRef
{
public:
    PagedVectorRef(const nautilus::val<PagedVector*>& pagedVectorRef, Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout);

    /**
     * @brief Writes a new record to the PagedVectorRef
     * @param record
     */
    void writeRecord(const Record& record);

    /**
     * @brief Reads a record in the PagedVectorRef
     * @param pos
     * @return Record
     */
    Record readRecord(const nautilus::val<uint64_t>& pos) const;

    /**
     * @brief Reads the keys from a record in the PagedVectorRef
     * @param pos
     * @return Record
     */
    Record readRecordKeys(const nautilus::val<uint64_t>& pos) const;

    /**
     * @brief Creates a PageVectorRefIter that points to the first entry in the PagedVectorRef
     * @return PagedVectorRefIter
     */
    PagedVectorRefIter begin() const;

    /**
     * @brief Creates a PageVectorRefIter that points to the entry at the given position in the PagedVectorRef
     * @param pos
     * @return PagedVectorRefIter
     */
    PagedVectorRefIter at(const nautilus::val<uint64_t>& pos) const;

    /**
     * @brief Creates a PageVectorRefIter that points to the end of the PagedVectorRef
     * @return PagedVectorRefIter
     */
    PagedVectorRefIter end() const;

    /**
     * @brief Equality operator
     * @param other
     * @return Boolean
     */
    bool operator==(const PagedVectorRef& other) const;

private:
    const nautilus::val<PagedVector*> pagedVectorRef;
    const Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    nautilus::val<uint64_t> totalNumberOfEntries;
};

class PagedVectorRefIter
{
public:
    friend class PagedVectorRef;

    explicit PagedVectorRefIter(const PagedVectorRef& pagedVector);

    /**
     * @brief Dereference operator that returns the record at a given entry in the ListRef
     * @return Record
     */
    Record operator*() const;

    /**
     * @brief Pre-increment operator that first increments and then returns the reference
     * @return Reference
     */
    PagedVectorRefIter& operator++();

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
    void setPos(const nautilus::val<uint64_t>& newPos);

    nautilus::val<uint64_t> pos;
    const PagedVectorRef pagedVector;
};

} ///namespace NES::Nautilus::Interface
