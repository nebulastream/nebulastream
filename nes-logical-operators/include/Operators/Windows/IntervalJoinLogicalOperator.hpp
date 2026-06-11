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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Schema/Field.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>

namespace NES
{
class SerializableOperator;

/// Logical operator for a streaming interval join: pairs each left tuple `a` with
/// right tuples `b` where `b.ts \in [a.ts + lowerBound, a.ts + upperBound]`.
///
/// Bounds are signed millisecond offsets (int64_t) so they can express past/future
/// intervals symmetrically (e.g. `[-4 ms, -1 ms]` is purely past-anchored). The
/// runtime slice width derives as `W = upperBound - lowerBound`; the constructor
/// rejects `lowerBound >= upperBound` since the point-predicate case (W = 0) is
/// degenerate.
class IntervalJoinLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    explicit IntervalJoinLogicalOperator(
        WeakLogicalOperator self,
        LogicalFunction joinFunction,
        Windowing::TimeCharacteristic timeCharacteristic,
        int64_t lowerBound,
        int64_t upperBound,
        JoinLogicalOperator::JoinType joinType = JoinLogicalOperator::JoinType::INNER_JOIN);

    explicit IntervalJoinLogicalOperator(
        WeakLogicalOperator self,
        std::array<LogicalOperator, 2> children,
        LogicalFunction joinFunction,
        Windowing::TimeCharacteristic timeCharacteristic,
        int64_t lowerBound,
        int64_t upperBound,
        JoinLogicalOperator::JoinType joinType);

    [[nodiscard]] LogicalFunction getJoinFunction() const;
    [[nodiscard]] const Windowing::TimeCharacteristic& getTimeCharacteristic() const;
    [[nodiscard]] int64_t getLowerBound() const noexcept;
    [[nodiscard]] int64_t getUpperBound() const noexcept;
    [[nodiscard]] JoinLogicalOperator::JoinType getJoinType() const noexcept;
    [[nodiscard]] const UnqualifiedUnboundField& getStartField() const;
    [[nodiscard]] const UnqualifiedUnboundField& getEndField() const;

    [[nodiscard]] bool operator==(const IntervalJoinLogicalOperator& rhs) const;

    [[nodiscard]] IntervalJoinLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] IntervalJoinLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] IntervalJoinLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] std::array<LogicalOperator, 2> getBothChildren() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] IntervalJoinLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "IntervalJoin";
    LogicalFunction joinFunction;
    Windowing::TimeCharacteristic timeCharacteristic;
    int64_t lowerBound;
    int64_t upperBound;
    JoinLogicalOperator::JoinType joinType;
    std::optional<std::array<LogicalOperator, 2>> children;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    std::array<UnqualifiedUnboundField, 2> startEndFields = std::array{
        UnqualifiedUnboundField{Identifier::parse("start"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("end"), DataType::Type::UINT64}};

    TraitSet traitSet;
    friend Reflector<TypedLogicalOperator<IntervalJoinLogicalOperator>>;
};

template <>
struct Reflector<TypedLogicalOperator<IntervalJoinLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<IntervalJoinLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<IntervalJoinLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<IntervalJoinLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<IntervalJoinLogicalOperator>);
}

namespace NES::detail
{
struct ReflectedIntervalJoinLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    LogicalFunction joinFunction;
    Windowing::TimeCharacteristic timeCharacteristic;
    int64_t lowerBound = 0;
    int64_t upperBound = 0;
    JoinLogicalOperator::JoinType joinType = JoinLogicalOperator::JoinType::INNER_JOIN;
};
}
