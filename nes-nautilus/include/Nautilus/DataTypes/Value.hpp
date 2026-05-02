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

#include <map>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>

namespace NES
{

/// Unified physical-level value at the function/Record boundary.
///
/// A Value is always a (possibly degenerate) compound: it holds an ordered map
/// of suffix -> VarVal components. Scalar Values have a single component keyed
/// by the empty string (SCALAR_KEY); compound Values (e.g. Point) have one
/// component per logical sub-field (e.g. ".x", ".y", ".z").
///
/// The empty-suffix convention lets Record::write spread Value components into
/// flat record fields uniformly — scalars write to `fieldName + ""`, compounds
/// to `fieldName + ".x"` etc. — so the operator and function code paths are the
/// same regardless of compound-ness.
///
/// For ergonomics, a Value implicitly converts from VarVal (yielding a scalar
/// Value) and forwards the standard arithmetic / comparison / boolean operators
/// to its scalar component. Compound Values do not support these operators —
/// callers must use component() or components() explicitly.
class Value
{
public:
    static constexpr std::string_view SCALAR_KEY = "";

    /// Build a scalar Value (single component keyed by SCALAR_KEY).
    static Value scalar(VarVal v);

    /// Build a Value with arbitrary components. Caller owns the suffix scheme.
    explicit Value(std::map<std::string, VarVal> components);

    /// Implicit scalar conversion from VarVal so existing function bodies that
    /// do `Value v = leftFn.execute(...)` and operate on `v` keep compiling.
    /// NOLINTNEXTLINE(google-explicit-constructor)
    Value(VarVal v) : Value(scalar(std::move(v))) { }

    [[nodiscard]] bool isScalar() const;
    [[nodiscard]] const VarVal& asScalar() const;
    [[nodiscard]] const VarVal& component(std::string_view suffix) const;
    [[nodiscard]] const std::map<std::string, VarVal>& components() const;

    /// Migration shim: lets a scalar Value flow back to VarVal contexts (memory
    /// I/O, hash, aggregation state) without per-line .asScalar() calls. Asserts
    /// on compound Values — those callers must be made compound-aware explicitly.
    /// NOLINTNEXTLINE(google-explicit-constructor)
    operator const VarVal&() const { return asScalar(); }

    /// Forwarded VarVal API for the scalar case. Each of these asserts isScalar()
    /// at the boundary; compound arithmetic must be expressed via components().
    template <typename T>
    [[nodiscard]] T getRawValueAs() const
    {
        return asScalar().getRawValueAs<T>();
    }

    [[nodiscard]] Value castToType(DataType::Type type) const;
    [[nodiscard]] nautilus::val<bool> isNull() const;
    [[nodiscard]] bool isNullable() const;

    explicit operator bool() const;

    Value operator+(const Value& other) const;
    Value operator-(const Value& other) const;
    Value operator*(const Value& other) const;
    Value operator/(const Value& other) const;
    Value operator%(const Value& other) const;
    Value operator==(const Value& other) const;
    Value operator!=(const Value& other) const;
    Value operator&&(const Value& other) const;
    Value operator||(const Value& other) const;
    Value operator<(const Value& other) const;
    Value operator>(const Value& other) const;
    Value operator<=(const Value& other) const;
    Value operator>=(const Value& other) const;
    Value operator&(const Value& other) const;
    Value operator|(const Value& other) const;
    Value operator^(const Value& other) const;
    Value operator<<(const Value& other) const;
    Value operator>>(const Value& other) const;
    Value operator!() const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const Value& value);

private:
    std::map<std::string, VarVal> componentsMap;
};

}
