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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_FIXEDPAGE_FIXEDPAGEREF_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_FIXEDPAGE_FIXEDPAGEREF_HPP_

#include <nautilus/val.hpp>
#include <Nautilus/Interface/FixedPage/FixedPage.hpp>

namespace NES::Nautilus::Interface {
// Forward declaration
class FixedPageRefIter;

/**
 * @brief This is a Nautilus wrapper for the FixedPage.
 * It wraps a MemRef to the underling data structure and provides access methods.
 */
class FixedPageRef {
  public:
    /**
     * @brief Constructs the wrapper.
     * @param fixedPageRef MemRef to the FixedPage
     * @param sizeOfRecord size of one record
     */
    FixedPageRef(const Nautilus::MemRef& fixedPageRef);

    /**
     * @brief Allocates an new entry and returns a reference to it
     * @param hash
     * @return Nautilus::MemRef
     */
    Nautilus::MemRef allocateEntry(const Nautilus::UInt64& hash);

    /**
     * @brief Getter for sizeOfRecord
     * @return Nautilus::UInt64 sizeOfRecord
     */
    Nautilus::UInt64 getSizeOfRecord();

    /**
     * @brief Getter for data
     * @return Nautilus::MemRef data
     */
    Nautilus::MemRef getDataPtr();

    /**
     * @brief Getter for currentPos
     * @return Nautilus::UInt64 currentPos
     */
    Nautilus::UInt64 getCurrentPos();

    /**
     * @brief Getter for capacity
     * @return Nautilus::UInt64 capacity
     */
    Nautilus::UInt64 getCapacity();

    /**
     * @brief Setter for currentPos
     * @param pos
     */
    void setCurrentPos(const Nautilus::UInt64& pos);

    /**
     * @brief Returns an iterator that points to the begin of this FixedPageRef
     * @return FixedPageRefIter
     */
    FixedPageRefIter begin();

    /**
     * @brief Returns an iterator pointing to the entry at pos
     * @param pos
     * @return FixedPageRefIter
     */
    FixedPageRefIter at(const Nautilus::UInt64& pos);

    /**
     * @brief Returns an iterator that points to the end of this FixedPageRef
     * @return FixedPageRefIter
     */
    FixedPageRefIter end();

    /**
     * @brief Equality operator
     * @param other
     * @return Boolean
     */
    bool operator==(const FixedPageRef& other) const;

  private:
    Nautilus::MemRef fixedPageRef;
};

class FixedPageRefIter {
  public:
    friend class FixedPageRef;

    /**
     * @brief Constructor
     * @param fixedPageRef
     */
    explicit FixedPageRefIter(FixedPageRef& fixedPageRef);

    /**
     * @brief Copy constructor
     * @param it
     */
    FixedPageRefIter(const FixedPageRefIter& it);

    /**
     * @brief Assignment operator
     * @param it
     * @return Reference to FixedPageRefIter
     */
    FixedPageRefIter& operator=(const FixedPageRefIter& it);

    /**
     * @brief Dereference operator that points to a given entry in the FixedPageRef
     * @return Nautilus::MemRef
     */
    Nautilus::MemRef operator*();

    /**
     * @brief Pre-increment operator that first increments and then returns the reference
     * @return Reference to FixedPageRefIter
     */
    FixedPageRefIter& operator++();

    /**
     * @brief Post-increment count that first returns the reference and then increments
     * @return FixedPageRefIter
     */
    FixedPageRefIter operator++(int);

    /**
     * @brief Equality operator
     * @param other
     * @return Boolean
     */
    bool operator==(const FixedPageRefIter& other) const;

    /**
     * @brief Inequality operator
     * @param other
     * @return Boolean
     */
    bool operator!=(const FixedPageRefIter& other) const;

  private:
    Nautilus::MemRef addr;
    FixedPageRef fixedPageRef;
};
}// namespace NES::Nautilus::Interface

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_FIXEDPAGE_FIXEDPAGEREF_HPP_
