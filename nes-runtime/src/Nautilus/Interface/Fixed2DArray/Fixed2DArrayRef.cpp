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

#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArray.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArrayRef.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Interface {

Fixed2DArrayRef::Fixed2DArrayRef(const Value<MemRef>& fixed2DArrayRef, const uint64_t& entrySize, const uint64_t& numCols)
    : fixed2DArrayRef(fixed2DArrayRef), entrySize(entrySize), sizeOfOneRow(entrySize * numCols) {}

Fixed2DArrayRowRef Fixed2DArrayRef::operator[](const Value<>& rowIndex) const {
    auto numRows = getMember(fixed2DArrayRef, Fixed2DArray, numRows).load<UInt64>();
    NES_ASSERT2_FMT(numRows > rowIndex, "Trying to access a row, which does not exist!");

    auto data = getMember(fixed2DArrayRef, Fixed2DArray, data).load<MemRef>();
    auto baseAddress = (data + (rowIndex * sizeOfOneRow)).as<MemRef>();
    return {baseAddress, entrySize};
}

Fixed2DArrayRowRef::Fixed2DArrayRowRef(Value<MemRef>& baseAddress, uint64_t entrySize)
    : baseAddress(baseAddress), entrySize(entrySize) {}

Value<MemRef> Fixed2DArrayRowRef::operator[](const Value<>& colIndex) const {
    auto offSet = (colIndex * entrySize);
    return (baseAddress + offSet).as<MemRef>();
}

}// namespace NES::Nautilus::Interface