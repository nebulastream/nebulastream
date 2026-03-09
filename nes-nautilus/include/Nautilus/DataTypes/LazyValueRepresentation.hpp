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
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Util/Strings.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
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
class LazyValueRepresentation;

/// Implementations for binary functions with the VarVal as lhs.
/// Will call an invoke of the corresponding reverse<opname> method of the LazyValueRepresentation
VarVal operator+(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator-(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator*(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator/(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator%(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator==(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator!=(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator&&(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator||(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator<(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator>(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator<=(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator>=(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator&(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator|(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator^(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator<<(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator>>(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);

/// Like the VariableSizedData type, values of this data type consist of a pointer to the location of the value and the size of the value.
/// Additionally, it stores its underlying datatype as member.
/// All field values are initially represented this way until the first emit operator.
/// In an emit, a lazy value will be parsed into its underlying nautilus value.
/// All logical functions are implemented for lazy values. Per default, the lazy value will be parsed and the function will be applied on the underlying values.
/// Derived classes of lazyValueRepresentation, which correspond to an implicit type, may override certain functions to avoid parsing.
/// By not parsing the value immediately into the internal format and instead just referring to the "raw string" value in the input-formatted buffer,
/// we potentially avoid costly parsing operations.
/// These values do not need to be "reverse-parsed" into their string format in the OutputFormatter, which especially saves parsing
/// costs in stateless queries with only one intermediate pipeline between source and sink.
class LazyValueRepresentation
{
public:
    explicit LazyValueRepresentation(
        const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size, const DataType& type, const nautilus::val<bool>& isNull)
        : size(size), ptrToLazyValue(reference), type(type), isNull(isNull)
    {
    }

    LazyValueRepresentation(const LazyValueRepresentation& other) = default;
    virtual ~LazyValueRepresentation() = default;

    LazyValueRepresentation(LazyValueRepresentation&& other) noexcept
        : size(other.size), ptrToLazyValue(other.ptrToLazyValue), type(other.type), isNull(other.isNull)
    {
    }

    LazyValueRepresentation& operator=(const LazyValueRepresentation& other) noexcept
    {
        if (this == &other || other.ptrToLazyValue == nullptr)
        {
            return *this;
        }

        size = other.size;
        ptrToLazyValue = other.ptrToLazyValue;
        isNull = other.isNull;
        return *this;
    }

    [[nodiscard]] nautilus::val<int8_t*> getContent() const { return ptrToLazyValue; }

    [[nodiscard]] nautilus::val<uint64_t> getSize() const { return size; }

    [[nodiscard]] nautilus::val<bool> getIsNull() const { return isNull; }

    [[nodiscard]] DataType getType() const { return type; }

    /// Method to check if the lazy value has any text behind it.
    /// Usable for some bool function overrides
    [[nodiscard]] nautilus::val<bool> isValid() const
    {
        return size > 0 && ptrToLazyValue != nullptr;
    }

    /// Method to check if the lazy values content forms a representation of a boolean true
    /// Usable for some bool function overrides
    [[nodiscard]] nautilus::val<bool> isBooleanTrue() const
    {
        return (getSize() == 1 && nautilus::memcmp(getContent(), nautilus::val<const char*>("1"), 1) == 0)
            || (size == 4
                && (nautilus::memcmp(getContent(), nautilus::val<const char*>("true"), 4) == 0
                    || nautilus::memcmp(getContent(), nautilus::val<const char*>("TRUE"), 4) == 0
                    || nautilus::memcmp(getContent(), nautilus::val<const char*>("True"), 4) == 0));
    }

    LazyValueRepresentation& operator=(LazyValueRepresentation&& other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        size = other.size;
        ptrToLazyValue = other.ptrToLazyValue;
        isNull = other.isNull;
        return *this;
    }

    /// Converts the lazy value into a VarVal of the underlying value, as dictated by the type member.
    [[nodiscard]] VarVal parseValue() const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const LazyValueRepresentation& lazyValue);

    /// Overridable logical function implementations. Per default, a lazy value must always be parsed so the function can be executed.
    [[nodiscard]] virtual VarVal operator+(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseAdd(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator-(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseSub(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator*(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseMul(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator/(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseDiv(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator%(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseMod(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator==(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseEQ(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator!=(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseNEQ(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator&&(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseAND(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator||(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseOR(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator<(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseLT(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator>(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseGT(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator<=(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseLE(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator>=(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseGE(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator&(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseBAND(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator|(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseBOR(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator^(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseXOR(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator<<(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseSHL(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator>>(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseSHR(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator!() const;

protected:
    nautilus::val<uint64_t> size;
    nautilus::val<int8_t*> ptrToLazyValue;
    DataType type;
    /// Usually, only a varval should be able to include this information. However, as sometimes, the lazyvalue itself casts itself into a different varval, we need to make an exception.
    nautilus::val<bool> isNull;
};
}
