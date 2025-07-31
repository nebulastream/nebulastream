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
#include <span>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>

namespace NES
{

}

template <>
struct fmt::formatter<NES::Schema<NES::QualifiedUnboundField, NES::Ordered>> : fmt::ostream_formatter
{
};

template <>
struct fmt::formatter<NES::Schema<NES::UnqualifiedUnboundField, NES::Unordered>> : fmt::ostream_formatter
{
};

template <>
struct fmt::formatter<NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>> : fmt::ostream_formatter
{
};

static_assert(fmt::formattable<NES::Schema<NES::UnboundFieldBase<std::dynamic_extent>, NES::Ordered>>);
