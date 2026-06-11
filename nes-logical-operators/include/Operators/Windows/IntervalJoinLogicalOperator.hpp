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
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <Windowing/WindowMetaData.hpp>

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
/// degenerate (see phase-3 §6 in interval-join-port-plan/).
class IntervalJoinLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    /// TODO #1471: PR #1471 will add LEFT_OUTER / RIGHT_OUTER / FULL_OUTER variants.
    /// For the initial port we only support INNER_JOIN; the constructor INVARIANTs on it.
    explicit IntervalJoinLogicalOperator(
        WeakLogicalOperator self,
        LogicalFunction joinFunction,
        Windowing::TimeCharacteristic timeCharacteristic,
        int64_t lowerBound,
        int64_t upperBound,
        JoinLogicalOperator::JoinType joinType = JoinLogicalOperator::JoinType::INNER_JOIN);

    [[nodiscard]] LogicalFunction getJoinFunction() const;
    [[nodiscard]] Schema getLeftSchema() const;
    [[nodiscard]] Schema getRightSchema() const;
    [[nodiscard]] const Windowing::TimeCharacteristic& getTimeCharacteristic() const;
    [[nodiscard]] int64_t getLowerBound() const noexcept;
    [[nodiscard]] int64_t getUpperBound() const noexcept;
    [[nodiscard]] JoinLogicalOperator::JoinType getJoinType() const noexcept;
    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;
    [[nodiscard]] const WindowMetaData& getWindowMetaData() const;

    [[nodiscard]] bool operator==(const IntervalJoinLogicalOperator& rhs) const;

    [[nodiscard]] IntervalJoinLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] IntervalJoinLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] IntervalJoinLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    static constexpr std::string_view NAME = "IntervalJoin";
    LogicalFunction joinFunction;
    Windowing::TimeCharacteristic timeCharacteristic;
    int64_t lowerBound;
    int64_t upperBound;
    WindowMetaData windowMetaData;
    JoinLogicalOperator::JoinType joinType;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema leftInputSchema, rightInputSchema, outputSchema;

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
    TypedLogicalOperator<IntervalJoinLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<IntervalJoinLogicalOperator>);
}

namespace NES::detail
{
struct ReflectedIntervalJoinLogicalOperator
{
    std::optional<LogicalFunction> joinFunction;
    Reflected timeCharacteristic;
    int64_t lowerBound = 0;
    int64_t upperBound = 0;
    JoinLogicalOperator::JoinType joinType = JoinLogicalOperator::JoinType::INNER_JOIN;
};
}
