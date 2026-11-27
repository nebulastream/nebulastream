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
#include <expected>
#include <string>
#include <string_view>
#include <variant>

#include <DataTypes/DataType.hpp>
#include <Types/NESType.hpp>
#include <Util/Any.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Open `TypeTrait` markers categorizing numeric, integer, and floating-point scalar `NESType`s.
struct NumericTypeTrait
{
};

struct IntegerTypeTrait
{
    bool isSigned = false;
};

struct FloatTypeTrait
{
};

/// Distinct C++ `NESTypeConcept` implementations for each built-in non-nullable scalar type.
/// Each type constructs its own strongly-typed C++ value representation (`int32_t`, `float`, `std::string`, etc.),
/// while nullability is modeled orthogonally via `OptionalType`.

struct BooleanType
{
    static constexpr std::string_view NAME = "BOOLEAN";
    using ValueType = bool;

    bool operator==(const BooleanType&) const = default;
    [[nodiscard]] std::expected<bool, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(bool value);
    [[nodiscard]] static NESFunctor functor();
};

struct CharType
{
    static constexpr std::string_view NAME = "CHAR";
    using ValueType = char;

    bool operator==(const CharType&) const = default;
    [[nodiscard]] std::expected<char, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(char value);
    [[nodiscard]] static NESFunctor functor();
};

struct Int8Type
{
    static constexpr std::string_view NAME = "INT8";
    using ValueType = int8_t;

    bool operator==(const Int8Type&) const = default;
    [[nodiscard]] std::expected<int8_t, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(int8_t value);
    [[nodiscard]] static NESFunctor functor();
};

struct Int16Type
{
    static constexpr std::string_view NAME = "INT16";
    using ValueType = int16_t;

    bool operator==(const Int16Type&) const = default;
    [[nodiscard]] std::expected<int16_t, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(int16_t value);
    [[nodiscard]] static NESFunctor functor();
};

struct Int32Type
{
    static constexpr std::string_view NAME = "INT32";
    using ValueType = int32_t;

    bool operator==(const Int32Type&) const = default;
    [[nodiscard]] std::expected<int32_t, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(int32_t value);
    [[nodiscard]] static NESFunctor functor();
};

struct Int64Type
{
    static constexpr std::string_view NAME = "INT64";
    using ValueType = int64_t;

    bool operator==(const Int64Type&) const = default;
    [[nodiscard]] std::expected<int64_t, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(int64_t value);
    [[nodiscard]] static NESFunctor functor();
};

struct UInt8Type
{
    static constexpr std::string_view NAME = "UINT8";
    using ValueType = uint8_t;

    bool operator==(const UInt8Type&) const = default;
    [[nodiscard]] std::expected<uint8_t, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(uint8_t value);
    [[nodiscard]] static NESFunctor functor();
};

struct UInt16Type
{
    static constexpr std::string_view NAME = "UINT16";
    using ValueType = uint16_t;

    bool operator==(const UInt16Type&) const = default;
    [[nodiscard]] std::expected<uint16_t, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(uint16_t value);
    [[nodiscard]] static NESFunctor functor();
};

struct UInt32Type
{
    static constexpr std::string_view NAME = "UINT32";
    using ValueType = uint32_t;

    bool operator==(const UInt32Type&) const = default;
    [[nodiscard]] std::expected<uint32_t, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(uint32_t value);
    [[nodiscard]] static NESFunctor functor();
};

struct UInt64Type
{
    static constexpr std::string_view NAME = "UINT64";
    using ValueType = uint64_t;

    bool operator==(const UInt64Type&) const = default;
    [[nodiscard]] std::expected<uint64_t, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(uint64_t value);
    [[nodiscard]] static NESFunctor functor();
};

struct Float32Type
{
    static constexpr std::string_view NAME = "FLOAT32";
    using ValueType = float;

    bool operator==(const Float32Type&) const = default;
    [[nodiscard]] std::expected<float, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(float value);
    [[nodiscard]] static NESFunctor functor();
};

struct Float64Type
{
    static constexpr std::string_view NAME = "FLOAT64";
    using ValueType = double;

    bool operator==(const Float64Type&) const = default;
    [[nodiscard]] std::expected<double, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(double value);
    [[nodiscard]] static NESFunctor functor();
};

struct VarSizedType
{
    static constexpr std::string_view NAME = "VARSIZED";
    using ValueType = std::string;

    bool operator==(const VarSizedType&) const = default;
    [[nodiscard]] std::expected<std::string, Exception> construct(const NESValueParameters& params) const;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESValue createValue(std::string value);
    [[nodiscard]] static NESFunctor functor();
};

struct UndefinedType
{
    static constexpr std::string_view NAME = "UNDEFINED";
    using ValueType = std::monostate;

    bool operator==(const UndefinedType&) const = default;

    [[nodiscard]] static NESType makeNESType();
    [[nodiscard]] static NESFunctor functor();
};

/// Instantiated `OPTIONAL(INNER_TYPE)` algebraic type constructor wrapping `innerType` with nullability.
struct OptionalType
{
    NESType innerType;

    bool operator==(const OptionalType& other) const = default;
    [[nodiscard]] const NESType& getInnerType() const noexcept { return innerType; }
    [[nodiscard]] std::expected<ExplicitAny, Exception> construct(const NESValueParameters& params) const;
    [[nodiscard]] bool equalsValue(const ExplicitAny& lhs, const ExplicitAny& rhs) const;

    [[nodiscard]] static NESType make(NESType innerType);
    [[nodiscard]] static NESValue createNullValue(NESType innerType);
    [[nodiscard]] static NESFunctor functor();
};

/// Helpers for inspecting and conditionally wrapping `OptionalType` on scalar `NESType`s.
[[nodiscard]] bool isOptionalNESType(const NESType& type) noexcept;
[[nodiscard]] const NESType& unwrapOptionalNESType(const NESType& type) noexcept;
[[nodiscard]] NESType wrapOptionalNESTypeIf(NESType innerType, bool nullable);

/// Typed extraction helpers over built-in scalar `NESValue`s (`Int8..Int64`, `UInt8..UInt64`, `Float32`, `Float64`).
[[nodiscard]] std::expected<int64_t, Exception> tryExtractSignedInteger(const NESValue& value);
[[nodiscard]] std::expected<uint64_t, Exception> tryExtractUnsignedInteger(const NESValue& value);
[[nodiscard]] std::expected<double, Exception> tryExtractFloatingPoint(const NESValue& value);

}
