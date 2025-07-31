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
#include "SchemaBase.hpp"
#include "UnboundField.hpp"

namespace NES
{
using UnboundOrderedSchema = SchemaBase<UnboundFieldBase<std::dynamic_extent>, true>;

}

template <>
struct fmt::formatter<NES::UnboundOrderedSchema> : fmt::ostream_formatter
{
};
template <>
struct fmt::formatter<NES::SchemaBase<NES::UnboundFieldBase<1>, false>> : fmt::ostream_formatter
{
};

template <>
struct fmt::formatter<NES::SchemaBase<NES::UnboundFieldBase<1>, true>> : fmt::ostream_formatter
{
};

static_assert(fmt::formattable<NES::SchemaBase<NES::UnboundFieldBase<std::dynamic_extent>, true>>);
