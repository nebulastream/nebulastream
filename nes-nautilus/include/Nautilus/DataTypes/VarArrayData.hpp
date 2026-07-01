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

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <DataTypes/DataType.hpp>
#include <nautilus/std/sstream.h>
#include <nautilus/val.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Forward declaration of VarVal to avoid cycling dependencies
class VarVal;

/// Variable-sized array of (currently primitive) nautilus values.
/// Unlike FixedSizedData, the number of elements are not known before receiving the array. However, we know the element type at trace time.
/// While VARSIZED is used for non-separable byte sequences (strings), this type supports indexing.
class VarArrayData
{
public:
    VarArrayData(const nautilus::val<int8_t*>& reference, DataType::Type elementType, const nautilus::val<uint64_t>& size);
    VarArrayData(const VarArrayData& other) = default;
    VarArrayData(VarArrayData&& other) noexcept = default;
    VarArrayData& operator=(const VarArrayData& other) = default;
    VarArrayData& operator=(VarArrayData&& other) noexcept = default;
    ~VarArrayData() = default;

    [[nodiscard]] VarVal at(const nautilus::val<uint64_t>& index);
    void writeAt(const nautilus::val<uint64_t>& index, const VarVal& value);

    /// This has to be figured out after trace time, thus differs from its sibling in FixedSizedData
    [[nodiscard]] nautilus::val<uint64_t> getNumElements() const;

    [[nodiscard]] DataType::Type getElementType() const;
    [[nodiscard]] nautilus::val<uint64_t> getTotalSizeInBytes() const;
    [[nodiscard]] nautilus::val<int8_t*> getRawPtr() const;

    /// Two VarArrayData are equal if element type, count, and the underlying
    /// bytes (memcmp over numElements * sizeof(element)) all match.
    nautilus::val<bool> operator==(const VarArrayData& rhs) const;
    nautilus::val<bool> operator!=(const VarArrayData& rhs) const;
    nautilus::val<bool> operator!() const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const VarArrayData& varSizedData);

private:
    nautilus::val<uint8_t*> ptr;
    nautilus::val<uint64_t> size;
    nautilus::val<uint64_t> numElements;
    DataType::Type elementType;
};
}
