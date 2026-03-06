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
#include <ranges>
#include <Schema/Field.hpp>
#include "Operators/LogicalOperator.hpp"

namespace NES
{

template <typename UnboundType>
struct Binder
{
    LogicalOperator producedBy;

    UnboundType operator()(UnboundType unboundType) const { return unboundType; }
};

template <typename UnboundType>
auto bind(LogicalOperator producedBy, UnboundType&& unboundType) {
    return Binder<std::remove_cvref_t<UnboundType>>{producedBy}(std::forward<UnboundType>(unboundType));
}

template <typename BoundType>
struct Unbinder
{
    constexpr BoundType operator()(BoundType boundType) const { return boundType; }
};

template <typename BoundType>
auto unbind(BoundType&& boundType) {
    return Unbinder<std::remove_cvref_t<BoundType>>{}(std::forward<BoundType>(boundType));
}

template <>
struct Binder<UnqualifiedUnboundField>
{
    LogicalOperator producedBy;

    Field operator()(const UnqualifiedUnboundField& unboundField)
    {
        return Field{producedBy, unboundField.getFullyQualifiedName(), unboundField.getDataType()};
    }
};

template <>
struct Binder<Identifier>
{
    LogicalOperator producedBy;

    Field operator()(const Identifier& name) const
    {
        const auto foundField = producedBy->getOutputSchema()[name];
        if (foundField.has_value())
        {
            return foundField.value();
        }
        PRECONDITION(false, "Field not found in producedBy's output schema");
        std::unreachable();
    }
};

template <>
struct Unbinder<Field>
{
    UnqualifiedUnboundField operator()(const Field& field) const { return field.unbound(); }
};

template <typename Left, typename Right>
struct Binder<std::pair<Left, Right>>
{
    using LeftOutput = decltype(std::declval<Binder<Left>>()(std::declval<Left>()));
    using RightOutput = decltype(std::declval<Binder<Right>>()(std::declval<Right>()));

    LogicalOperator producedBy;

    std::pair<LeftOutput, RightOutput> operator()(std::pair<Left, Right> pair)
    {
        return {Binder<Left>{producedBy}(pair.first), Binder<Right>{producedBy}(pair.second)};
    }
};

template <typename Left, typename Right>
struct Unbinder<std::pair<Left, Right>>
{
    using LeftOutput = decltype(std::declval<Unbinder<Left>>()(std::declval<Left>()));
    using RightOutput = decltype(std::declval<Unbinder<Right>>()(std::declval<Right>()));

    std::pair<LeftOutput, RightOutput> operator()(const std::pair<Left, Right>& pair) const
    {
        return {Unbinder<Left>{}(pair.first), Unbinder<Right>{}(pair.second)};
    }
};

struct RangeBinder
{
    LogicalOperator producedBy;

    template <std::ranges::viewable_range Range>
    constexpr auto operator()(Range&& range) const
    {
        return std::views::transform(std::forward<Range>(range), Binder<std::ranges::range_value_t<Range>>{producedBy});
    }
};

template <std::ranges::viewable_range Range>
constexpr auto operator|(Range&& range, const RangeBinder& binder)
{
    return binder(std::forward<Range>(range));
}

struct RangeUnbinder
{
    template <std::ranges::viewable_range Range>
    constexpr auto operator()(Range&& range) const
    {
        return std::views::transform(std::forward<Range>(range), Unbinder<std::ranges::range_value_t<Range>>{});
    }
};

template <std::ranges::viewable_range Range>
constexpr auto operator|(Range&& range, const RangeUnbinder& unbinder)
{
    return unbinder(std::forward<Range>(range));
}

template <std::ranges::viewable_range UnboundRange>
requires (!std::same_as<UnboundRange, Schema<std::ranges::range_value_t<UnboundRange>, Ordered>> &&
         !std::same_as<UnboundRange, Schema<std::ranges::range_value_t<UnboundRange>, Unordered>>)
struct Binder<UnboundRange>
{
    LogicalOperator producedBy;

    auto operator()(UnboundRange range) { return RangeBinder{producedBy}(std::move(range)); }
};

template <std::ranges::viewable_range BoundRange>
requires (!std::same_as<std::remove_cvref_t<BoundRange>, Schema<std::ranges::range_value_t<BoundRange>, Ordered>> &&
         !std::same_as<std::remove_cvref_t<BoundRange>, Schema<std::ranges::range_value_t<BoundRange>, Unordered>>)
struct Unbinder<BoundRange>
{
    auto operator()(BoundRange range) { return RangeUnbinder{}(std::move(range)); }
};

}