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
#include <concepts>
#include <Identifiers/Identifier.hpp>
#include <folly/hash/Hash.h>

#include "SchemaBase.hpp"

namespace NES
{
template <size_t IdListExtent>
struct UnboundFieldBase
{
    UnboundFieldBase(IdentifierListBase<IdListExtent> name, DataType dataType) : name(std::move(name)), dataType(std::move(dataType)) { }

    UnboundFieldBase(IdentifierListBase<IdListExtent> name, DataType::Type dataType)
        : name(std::move(name)), dataType(DataTypeProvider::provideDataType(dataType))
    {
    }

    template <size_t OtherIdListExtent>
    UnboundFieldBase(const UnboundFieldBase<OtherIdListExtent>& other)
    requires(IdListExtent == std::dynamic_extent || IdListExtent != OtherIdListExtent)
        : name(other.getFullyQualifiedName()), dataType(other.getDataType())
    {
    }

    [[nodiscard]] const IdentifierListBase<IdListExtent>& getFullyQualifiedName() const { return name; }

    [[nodiscard]] const DataType& getDataType() const { return dataType; }

    friend bool operator==(const UnboundFieldBase& lhs, const UnboundFieldBase& rhs)
    {
        return lhs.getFullyQualifiedName() == rhs.getFullyQualifiedName() && lhs.getDataType() == rhs.getDataType();
    }

    friend std::ostream& operator<<(std::ostream& os, const UnboundFieldBase& obj)
    {
        return os << fmt::format("QualifiedUnboundField: (name: {}, type: {})", obj.getFullyQualifiedName(), obj.getDataType());
    }

private:
    IdentifierListBase<IdListExtent> name;
    DataType dataType;
};

using QualifiedUnboundField = UnboundFieldBase<std::dynamic_extent>;
using UnqualifiedUnboundField = UnboundFieldBase<1>;

static_assert(std::convertible_to<UnqualifiedUnboundField, QualifiedUnboundField>);

template <size_t IdListExtent>
struct ReflectedUnboundFieldBase
{
    IdentifierListBase<IdListExtent> name;
    DataType dataType;
};

template <size_t IdListExtent>
struct Reflector<UnboundFieldBase<IdListExtent>>
{
    Reflected operator()(const UnboundFieldBase<IdListExtent>& field) const
    {
        return reflect(ReflectedUnboundFieldBase<IdListExtent>{field.getFullyQualifiedName(), field.getDataType()});
    }
};

template <size_t IdListExtent>
struct Unreflector<UnboundFieldBase<IdListExtent>>
{
    UnboundFieldBase<IdListExtent> operator()(const Reflected& rfl, const ReflectionContext& context) const
    {
        const auto unreflected = context.unreflect<ReflectedUnboundFieldBase<IdListExtent>>(rfl);
        return UnboundFieldBase<IdListExtent>(unreflected.name, unreflected.dataType);
    }
};
}

template <>
struct fmt::formatter<NES::UnqualifiedUnboundField> : fmt::ostream_formatter
{
};

template <>
struct fmt::formatter<NES::UnboundFieldBase<std::dynamic_extent>> : fmt::ostream_formatter
{
};

static_assert(fmt::formattable<NES::UnqualifiedUnboundField>);
static_assert(fmt::formattable<NES::UnboundFieldBase<std::dynamic_extent>>);

template <size_t IdListExtent>
struct std::hash<NES::UnboundFieldBase<IdListExtent>>
{
    size_t operator()(const NES::UnboundFieldBase<IdListExtent>& field) const noexcept
    {
        return folly::hash::hash_combine(field.getFullyQualifiedName(), field.getDataType());
    }
};
