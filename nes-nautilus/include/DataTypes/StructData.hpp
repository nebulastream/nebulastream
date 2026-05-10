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
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <nautilus/std/sstream.h>
#include <nautilus/val.hpp>

namespace NES
{

class VarVal;

/// Composite, in-place laid-out value for STRUCT data types.
///
/// Counterpart to FixedSizedData for nominal STRUCT layouts: the
/// (name, DataType) field list is plain C++ data so the trace can
/// constant-fold offsets, while the bytes themselves live behind
/// a single nautilus pointer.
///
/// Layout is *inline*: a primitive field occupies its native size,
/// a FIXEDSIZED field occupies `count * elementSize` bytes (no
/// offset/size indirection — that representation is for tuple
/// buffers, which the PoC doesn't touch yet), and a nested STRUCT
/// recurses with the same rules.
///
/// Domain-specific concretizations (e.g. `ThermalFrameData` in the
/// Image plugin) wrap this and expose typed accessors.
class StructData
{
public:
    StructData(const nautilus::val<int8_t*>& reference, std::vector<std::pair<std::string, DataType>> fields);
    StructData(const StructData& other) = default;
    StructData(StructData&& other) noexcept = default;
    StructData& operator=(const StructData& other) = default;
    StructData& operator=(StructData&& other) noexcept = default;
    ~StructData() = default;

    /// Loads the named field as a VarVal whose alternative matches the
    /// field's DataType. Throws `UnknownField` (at trace time) if no
    /// field with that name exists.
    [[nodiscard]] VarVal at(std::string_view fieldName) const;

    /// Loads the field at the given positional index (0-based). Throws
    /// `OutOfRangeAccess` if `index >= getNumFields()`.
    [[nodiscard]] VarVal at(size_t fieldIndex) const;

    /// Stores `value` into the named field at its inline offset.
    ///
    /// Primitive fields use `VarVal::writeToMemory` (the VarVal must carry a
    /// width-matching `nautilus::val<T>`). FIXEDSIZED and STRUCT fields are
    /// copied byte-for-byte from the source's `getRawPtr()` — the source's
    /// inline layout must match the field's declared layout, which is the
    /// caller's responsibility.
    ///
    /// Throws `FieldNotFound` for unknown names and `OutOfRangeAccess` for
    /// out-of-range indices, mirroring `at()`.
    void writeAt(std::string_view fieldName, const VarVal& value) const;
    void writeAt(size_t fieldIndex, const VarVal& value) const;

    [[nodiscard]] size_t getNumFields() const;
    [[nodiscard]] size_t getTotalSizeInBytes() const;
    [[nodiscard]] nautilus::val<int8_t*> getRawPtr() const;
    [[nodiscard]] const std::vector<std::pair<std::string, DataType>>& getFields() const;

    /// Two StructData are equal iff their field layouts are identical
    /// (names + types, in order) and the underlying bytes match
    /// (memcmp over getTotalSizeInBytes()).
    nautilus::val<bool> operator==(const StructData& rhs) const;
    nautilus::val<bool> operator!=(const StructData& rhs) const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const StructData& structData);

private:
    /// Byte size of a single field according to the inline layout rules.
    /// FIXEDSIZED fields are inline (count * elementSize), STRUCT fields
    /// recurse, primitives use `getSizeInBytesWithoutNull()`.
    static size_t fieldSizeInBytes(const DataType& fieldType);

    nautilus::val<int8_t*> ptr;
    std::vector<std::pair<std::string, DataType>> fields;
};

}
