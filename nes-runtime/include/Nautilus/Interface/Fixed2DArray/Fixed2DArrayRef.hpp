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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_FIXED2DARRAY_FIXED2DARRAYREF_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_FIXED2DARRAY_FIXED2DARRAYREF_HPP_

#include <Nautilus/Interface/DataTypes/Value.hpp>

#include <cstdint>
namespace NES::Nautilus::Interface {

class Fixed2DArrayRowRef;

/**
 * @brief This is a nautilus wrapper for Fixed2DArray. It wraps a memref to the underlying fixed array and provides access methods
 */
class Fixed2DArrayRef {

  public:
    /**
     * @brief Constructor for a Fixed2DArrayRef object
     * @param fixed2DArrayRef
     * @param entrySize
     */
    explicit Fixed2DArrayRef(const Value<MemRef>& fixed2DArrayRef, const uint64_t& entrySize, const uint64_t& numCols);

    /**
     * @brief Creates a row reference for the given rowIndex
     * @param rowIndex
     * @return Fixed2DArrayRowRef
     */
    Fixed2DArrayRowRef operator[](const Value<>& rowIndex) const;

  private:
    Value<MemRef> fixed2DArrayRef;
    const uint64_t entrySize;
    const uint64_t sizeOfOneRow;
};

/**
 * @brief This class represents a single row of the Fixed2DArray
 */
class Fixed2DArrayRowRef {
  public:
    /**
     * @brief Constructor for a Fixed2DArrayRowRef object
     * @param baseAddress
     * @param entrySize
     */
    Fixed2DArrayRowRef(Value<MemRef>& baseAddress, uint64_t entrySize);

    /**
     * @brief Accesses the given column in this row.
     * @param colIndex
     * @return MemRef to the start of the cell
     */
    Value<MemRef> operator[](const Value<>& colIndex) const;

  private:
    Value<MemRef> baseAddress;
    const uint64_t entrySize;
};

}// namespace NES::Nautilus::Interface
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_FIXED2DARRAY_FIXED2DARRAYREF_HPP_
