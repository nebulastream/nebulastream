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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_FIXED2DARRAY_FIXED2DARRAY_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_FIXED2DARRAY_FIXED2DARRAY_HPP_
#include <Runtime/Allocator/MemoryResource.hpp>

#include <cstdint>
namespace NES::Nautilus::Interface {

class Fixed2DArray;
class Fixed2DArrayRef;

/**
 * @brief This class represents a fixed two dimensional array for a dynamical entrySize. Once the size is determined, it can not
 * be increased or decreased.
 */
class Fixed2DArray {
  public:
    /**
     * @brief Constructor for a Fixed2DArray object that allocates the required memory
     * @param allocator
     * @param numRows
     * @param numCols
     * @param entrySize
     */
    explicit Fixed2DArray(std::pmr::memory_resource& allocator, uint64_t numRows, uint64_t numCols, uint64_t entrySize);

    /**
     * @brief Deconstructor
     */
    virtual ~Fixed2DArray();

  private:
    friend Fixed2DArrayRef;
    std::size_t entrySize;
    uint64_t numRows;
    uint64_t numCols;
    uint8_t* data;
};
}// namespace NES::Nautilus::Interface

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_FIXED2DARRAY_FIXED2DARRAY_HPP_
