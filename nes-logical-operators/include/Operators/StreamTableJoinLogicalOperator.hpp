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

#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/Variant.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>

namespace NES
{

namespace detail
{
template <typename T>
using StreamTableArrayOfTwo = std::array<T, 2>;
}

using StreamTableJoinTimeCharacteristics = NES::VariantContainerFrom<NES::detail::StreamTableArrayOfTwo, Windowing::TimeCharacteristic>;

/// An asymmetric join between a stream (left child) and a growing table (right child).
///
/// The table retains every tuple for the lifetime of the query. Stream tuples are
/// evaluated once the table watermark has passed their timestamp. If no time
/// characteristics are configured, stream tuples are retained until table EOS.
/// An inner join emits every matching pair; a left semi join emits the stream
/// tuple once when at least one eligible table tuple matches.
class StreamTableJoinLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        LEFT_SEMI_JOIN
    };

    explicit StreamTableJoinLogicalOperator(
        WeakLogicalOperator self,
        LogicalFunction joinFunction,
        std::optional<StreamTableJoinTimeCharacteristics> timeCharacteristics,
        JoinType joinType);

    explicit StreamTableJoinLogicalOperator(
        WeakLogicalOperator self,
        std::array<LogicalOperator, 2> children,
        LogicalFunction joinFunction,
        std::optional<StreamTableJoinTimeCharacteristics> timeCharacteristics,
        JoinType joinType);

    static TypedLogicalOperator<StreamTableJoinLogicalOperator> create(
        LogicalFunction joinFunction,
        std::optional<StreamTableJoinTimeCharacteristics> timeCharacteristics = std::nullopt,
        JoinType joinType = JoinType::INNER_JOIN);

    static TypedLogicalOperator<StreamTableJoinLogicalOperator> create(
        std::array<LogicalOperator, 2> children,
        LogicalFunction joinFunction,
        std::optional<StreamTableJoinTimeCharacteristics> timeCharacteristics = std::nullopt,
        JoinType joinType = JoinType::INNER_JOIN);

    [[nodiscard]] LogicalFunction getJoinFunction() const;
    [[nodiscard]] JoinType getJoinType() const;
    [[nodiscard]] std::optional<StreamTableJoinTimeCharacteristics> getTimeCharacteristics() const;
    [[nodiscard]] std::array<LogicalOperator, 2> getBothChildren() const;

    [[nodiscard]] bool operator==(const StreamTableJoinLogicalOperator& rhs) const;
    [[nodiscard]] StreamTableJoinLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] StreamTableJoinLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] StreamTableJoinLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] StreamTableJoinLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "StreamTableJoin";

    void inferLocalSchema();

    std::optional<std::array<LogicalOperator, 2>> children;
    LogicalFunction joinFunction;
    std::optional<StreamTableJoinTimeCharacteristics> timeCharacteristics;
    JoinType joinType;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
    TraitSet traitSet;

    friend struct std::hash<StreamTableJoinLogicalOperator>;
    friend struct Reflector<TypedLogicalOperator<StreamTableJoinLogicalOperator>>;
};

static_assert(LogicalOperatorConcept<StreamTableJoinLogicalOperator>);

template <>
struct Reflector<TypedLogicalOperator<StreamTableJoinLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<StreamTableJoinLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<StreamTableJoinLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<StreamTableJoinLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;

private:
    ContextType plan;
};

namespace detail
{
struct ReflectedStreamTableJoinLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    LogicalFunction joinFunction;
    std::optional<StreamTableJoinTimeCharacteristics> timeCharacteristics;
    StreamTableJoinLogicalOperator::JoinType joinType = StreamTableJoinLogicalOperator::JoinType::INNER_JOIN;
};
}

}

template <>
struct std::hash<NES::StreamTableJoinLogicalOperator>
{
    std::size_t operator()(const NES::StreamTableJoinLogicalOperator& op) const noexcept;
};
