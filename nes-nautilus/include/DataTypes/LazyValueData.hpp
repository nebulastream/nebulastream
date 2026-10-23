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

#include <cstdint>
#include <memory>
#include <ostream>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES
{
class VarVal;
class LazyValueData;

/// Deserializes the serialized bytes of a LazyValueData value. nes-nautilus only knows this interface. The concrete
/// implementation, which knows about the employed deserializer, lives in the nes-input-formatter component.
/// This avoids linking the nes-input-formatters library to nes-nautilus.
class LazyValueDeserializer
{
public:
    LazyValueDeserializer() noexcept = default;
    virtual ~LazyValueDeserializer() noexcept = default;

    [[nodiscard]] virtual VarVal deserialize(const nautilus::val<int8_t*>& ptr, const nautilus::val<uint64_t>& size) const = 0;
};

/// Implementations for binary functions with VarVal as lhs and LazyValueData as rhs.
/// They are implemented in here and not in VarVal, as VarVal should not hold the type-specific implementations of functions.
VarVal operator+(const VarVal& other, const LazyValueData& rhs);
VarVal operator-(const VarVal& other, const LazyValueData& rhs);
VarVal operator*(const VarVal& other, const LazyValueData& rhs);
VarVal operator/(const VarVal& other, const LazyValueData& rhs);
VarVal operator%(const VarVal& other, const LazyValueData& rhs);
VarVal operator==(const VarVal& other, const LazyValueData& rhs);
VarVal operator!=(const VarVal& other, const LazyValueData& rhs);
VarVal operator&&(const VarVal& other, const LazyValueData& rhs);
VarVal operator||(const VarVal& other, const LazyValueData& rhs);
VarVal operator<(const VarVal& other, const LazyValueData& rhs);
VarVal operator>(const VarVal& other, const LazyValueData& rhs);
VarVal operator<=(const VarVal& other, const LazyValueData& rhs);
VarVal operator>=(const VarVal& other, const LazyValueData& rhs);
VarVal operator&(const VarVal& other, const LazyValueData& rhs);
VarVal operator|(const VarVal& other, const LazyValueData& rhs);
VarVal operator^(const VarVal& other, const LazyValueData& rhs);
VarVal operator<<(const VarVal& other, const LazyValueData& rhs);
VarVal operator>>(const VarVal& other, const LazyValueData& rhs);

/// This internal type represents serialized data of an arbitrary input format. It consists of a pointer and a size, navigating to the address
/// in memory where the serialized value is located. The advantage of this representation is that it enables on-demand deserialization.
/// A query might not access every field of the stream schema, leading to unnecessary deserialize operations on these fields.
/// Therefore, the deserializers have the option to postpone the deserialization process until the deserialized value is actually needed.
///
/// This representation persists until the EMIT stage of the pipeline that contains the input-formatting scan. At this point, each remaining
/// value will be deserialized, so that we do not need to copy textual, formatted values into the buffers, which does not align with the internal size
/// of the schema. The innermost selections and projections will be executed by then, so potentially lesser values need to be deserialized at this point.
class LazyValueData
{
public:
    explicit LazyValueData(
        const nautilus::val<int8_t*>& ptr, const nautilus::val<uint64_t>& size, std::shared_ptr<const LazyValueDeserializer> deserializer);

    [[nodiscard]] nautilus::val<int8_t*> getContent() const { return ptr; }

    [[nodiscard]] nautilus::val<uint64_t> getSize() const { return size; }

    /// Uses the deserializer to deserialize the value and return the result.
    /// Nautilus could optimize duplicate deserialize operations out by itself, if we could tag the deserialization invokes with read only, will return, no unwind.
    /// As this would be the most elegant choice, we should implement it like this in the future.
    [[nodiscard]] VarVal deserializeValue() const;
    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const LazyValueData& variableSizedData);

    /// Basic binary and unary function implementations. Per default, a lazy value must always be deserialized so the function can be executed.
    [[nodiscard]] VarVal operator+(const VarVal& other) const;
    [[nodiscard]] VarVal operator-(const VarVal& other) const;
    [[nodiscard]] VarVal operator*(const VarVal& other) const;
    [[nodiscard]] VarVal operator/(const VarVal& other) const;
    [[nodiscard]] VarVal operator%(const VarVal& other) const;
    [[nodiscard]] VarVal operator==(const VarVal& other) const;
    [[nodiscard]] VarVal operator!=(const VarVal& other) const;
    [[nodiscard]] VarVal operator&&(const VarVal& other) const;
    [[nodiscard]] VarVal operator||(const VarVal& other) const;
    [[nodiscard]] VarVal operator<(const VarVal& other) const;
    [[nodiscard]] VarVal operator>(const VarVal& other) const;
    [[nodiscard]] VarVal operator<=(const VarVal& other) const;
    [[nodiscard]] VarVal operator>=(const VarVal& other) const;
    [[nodiscard]] VarVal operator&(const VarVal& other) const;
    [[nodiscard]] VarVal operator|(const VarVal& other) const;
    [[nodiscard]] VarVal operator^(const VarVal& other) const;
    [[nodiscard]] VarVal operator<<(const VarVal& other) const;
    [[nodiscard]] VarVal operator>>(const VarVal& other) const;
    [[nodiscard]] VarVal operator!() const;

private:
    nautilus::val<int8_t*> ptr;
    nautilus::val<uint64_t> size;
    std::shared_ptr<const LazyValueDeserializer> deserializer;
};
}
