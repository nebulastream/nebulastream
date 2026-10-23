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
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Util/Strings.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

namespace NES
{
class RecordBuffer;
class AbstractBufferProvider;
class VarVal;
class LazyValueData;
class ValueDeserializer;

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
/// of the schema. The innermost selections and projections will be executed by then, so potentially lesser values need deserialization at this point.
class LazyValueData
{
    struct DeserializerInformation
    {
        std::string deserializerKey;
        bool quoted{};
        bool hasTrailingSpaces{};
        DataType dataType;
        const std::vector<std::string>* nullValues{};
        nautilus::val<Arena*> arena;
    };

public:
    explicit LazyValueData(
        const nautilus::val<int8_t*>& ptr,
        const nautilus::val<uint64_t>& size,
        const std::string& deserializerName,
        const bool quoted,
        const bool hasTrailingSpaces,
        const DataType dataType,
        const std::vector<std::string>& nullValues,
        const nautilus::val<Arena*>& arena);

    [[nodiscard]] nautilus::val<int8_t*> getContent() const { return ptr; }

    [[nodiscard]] nautilus::val<uint64_t> getSize() const { return size; }

    /// Uses the deserializer to deserialize the value and return the result.
    /// The result is memoized in the deserializedValue cache, so the other reads of this value will not have to deserialize the lazy value
    /// again.
    /// Nautilus could optimize duplicate deserializes out by itself, if we could tag the deserialization invokes with read only, will return, no unwind.
    /// As this would be the most elegant choice, we should definitely implement it like this in the future and remove the deserializedValue cache.
    [[nodiscard]] VarVal deserializeValue() const;
    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const LazyValueData& variableSizedData);

    /// Basic binary and unary function implementations. Per default, a lazy value must always be deserialized so the function can be executed.
    /// Reverse<Operation> will execute operation(other, this) and is called out of the binary functions installed above.
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
    /// Holds the deserialized value once deserializeValue() ran. Defined in the .cpp, as VarVal is incomplete here.
    struct DeserializedValueCache;

    nautilus::val<int8_t*> ptr;
    nautilus::val<uint64_t> size;

    /// Accumulates all arguments required to create and call the deserializer.
    DeserializerInformation deserializerInfo;

    /// Shared between all copies of this lazy value, see deserializeValue()
    std::shared_ptr<DeserializedValueCache> deserializedValue;
};
}
