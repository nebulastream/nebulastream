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

#include <Nautilus/Interface/Fixed2DArray/Fixed2DArray.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Interface {

Fixed2DArray::Fixed2DArray(std::pmr::memory_resource& allocator, uint64_t numRows, uint64_t numCols, uint64_t entrySize)
    : entrySize(entrySize), numRows(numRows), numCols(numCols),
      data(static_cast<uint8_t*>(allocator.allocate(entrySize * numRows * numCols))) {
    NES_ASSERT2_FMT(entrySize > 0, "Entrysize for a pagedVector has to be larger than 0!");
    NES_ASSERT2_FMT(numRows > 0, "There has to be at least one row!");
    NES_ASSERT2_FMT(numCols > 0, "There has to be at least one column!");
}

Fixed2DArray::~Fixed2DArray() { std::free(data); }

}// namespace NES::Nautilus::Interface