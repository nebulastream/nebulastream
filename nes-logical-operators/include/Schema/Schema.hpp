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

#include <Identifiers/Identifier.hpp>


#include <concepts>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <ostream>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <boost/numeric/conversion/detail/bounds.hpp>
#include <ErrorHandling.hpp>
#include "DataTypes/SchemaBase.hpp"

namespace NES
{


static_assert(std::ranges::range<Schema<Field, Unordered>>);
static_assert(std::same_as<std::ranges::range_value_t<Schema<Field, Unordered>>, Field>);
static_assert(std::ranges::viewable_range<const Schema<Field, Unordered>&>);

template <typename FieldType, OrderType IsOrdered>
struct Binder<Schema<FieldType, IsOrdered>>
{
    using BoundFieldType = decltype(std::declval<Binder<FieldType>>()(std::declval<FieldType>()));
    LogicalOperator producedBy;

    Schema<BoundFieldType, IsOrdered> operator()(const Schema<FieldType, IsOrdered>& schema) const
    {
        return schema | RangeBinder{producedBy} | std::ranges::to<Schema<BoundFieldType, IsOrdered>>();
    }
};

template <typename FieldType, OrderType IsOrdered>
struct Unbinder<Schema<FieldType, IsOrdered>>
{
    using UnboundFieldType = decltype(std::declval<Unbinder<FieldType>>()(std::declval<FieldType>()));

    Schema<UnboundFieldType, IsOrdered> operator()(const Schema<FieldType, IsOrdered>& schema) const
    {
        return schema | RangeUnbinder{} | std::ranges::to<Schema<UnboundFieldType, IsOrdered>>();
    }
};

}

template <>
struct fmt::formatter<NES::Schema<NES::Field, NES::Unordered>> : fmt::ostream_formatter
{
};
