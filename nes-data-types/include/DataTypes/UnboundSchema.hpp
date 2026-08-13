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
#include <span>
#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>

namespace NES
{

/// Sums up the sizes of all fields, including their null bytes.
/// These are free functions instead of members of Schema, because Schema must not require its field type
/// to have a notion of size, only a getFullyQualifiedName() method.
[[nodiscard]] inline size_t getSizeInBytes(const Schema<QualifiedUnboundField, Ordered>& schema)
{
    return std::ranges::fold_left(
        schema, size_t{0}, [](const size_t acc, const auto& field) { return acc + field.getDataType().getSizeInBytesWithNull(); });
}

[[nodiscard]] inline size_t getSizeInBytes(const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    return std::ranges::fold_left(
        schema, size_t{0}, [](const size_t acc, const auto& field) { return acc + field.getDataType().getSizeInBytesWithNull(); });
}

}

/// The generic fmt::formatter partial specialization for Schema in Schema/Schema.hpp covers all
/// unbound schema instantiations; do not add full specializations here — they are ill-formed in any
/// translation unit that instantiates the formatter (e.g. via fmt::formattable checks in ConfigField)
/// before including this header.
static_assert(fmt::formattable<NES::Schema<NES::QualifiedUnboundField, NES::Ordered>>);
static_assert(fmt::formattable<NES::Schema<NES::UnqualifiedUnboundField, NES::Unordered>>);
static_assert(fmt::formattable<NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>>);
static_assert(fmt::formattable<NES::Schema<NES::UnboundFieldBase<std::dynamic_extent>, NES::Ordered>>);
