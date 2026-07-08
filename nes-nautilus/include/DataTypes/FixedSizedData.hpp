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
#include <DataTypes/DataType.hpp>
#include <nautilus/std/sstream.h>
#include <nautilus/val.hpp>

namespace NES
{

/// Forward declared to break the include cycle with `VarVal` (which holds
/// `FixedSizedData` as one of its variant alternatives). The `at()` accessor
/// returns `VarVal` by value, but only the .cpp needs the complete type.
class VarVal;

/// Fixed-size array of primitive nautilus values.
///
/// Counterpart to VariableSizedData for the case where the number of elements
/// and their primitive type are known at trace time. The size and element type
/// are plain C++ scalars (not nautilus::val) so that the trace can specialize
/// `at()` to a single typed load without runtime branching.
///
/// Only primitive element types from DataType::Type are supported; passing
/// VARSIZED or UNDEFINED to the constructor or accessor results in an error
/// from the underlying VarVal machinery.
class FixedSizedData
{
public:
    FixedSizedData(const nautilus::val<int8_t*>& reference, size_t numElements, const DataType& elementType);
    FixedSizedData(const FixedSizedData& other) = default;
    FixedSizedData(FixedSizedData&& other) noexcept = default;
    FixedSizedData& operator=(const FixedSizedData& other) = default;
    FixedSizedData& operator=(FixedSizedData&& other) noexcept = default;
    ~FixedSizedData() = default;

    /// Loads the element at `index` as a non-nullable VarVal carrying the
    /// primitive type given at construction. Performs a traced bounds check
    /// that throws `IndexOutOfBounds` at query runtime if `index >= numElements`.
    [[nodiscard]] VarVal at(const nautilus::val<uint64_t>& index) const;

    /// Stores `value`'s payload at `index`, advancing by `sizeof(elementType)`.
    /// `at()` returns a VarVal by value so it cannot be the LHS of an
    /// assignment — call `writeAt` to write a typed element. Same traced bounds
    /// check as `at()`. The VarVal must carry a primitive `nautilus::val<T>`
    /// whose width matches `elementType`; `VariableSizedData`, `FixedSizedData`,
    /// and `StructData` payloads are rejected by `VarVal::writeToMemory`.
    void writeAt(const nautilus::val<uint64_t>& index, const VarVal& value) const;

    [[nodiscard]] size_t getNumElements() const;
    [[nodiscard]] DataType getElementType() const;
    [[nodiscard]] size_t getTotalSizeInBytes() const;
    [[nodiscard]] nautilus::val<int8_t*> getRawPtr() const;

    /// Two FixedSizedData are equal iff element type, count, and the underlying
    /// bytes (memcmp over numElements * sizeof(element)) all match.
    nautilus::val<bool> operator==(const FixedSizedData& rhs) const;
    nautilus::val<bool> operator!=(const FixedSizedData& rhs) const;
    nautilus::val<bool> operator!() const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const FixedSizedData& fixedSizedData);

private:
    nautilus::val<int8_t*> ptr;
    size_t numElements;
    DataType elementType;
};

}
