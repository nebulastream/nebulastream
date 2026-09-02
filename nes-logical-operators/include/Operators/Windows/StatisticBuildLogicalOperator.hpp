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
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Serialization/WindowAggregationLogicalFunctionReflection.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <folly/hash/Hash.h>

namespace NES
{

struct StatisticAggregation
{
    WindowAggregationLogicalFunction function;
    StatisticId statisticId;

    bool operator==(const StatisticAggregation& rhs) const { return function == rhs.function && statisticId == rhs.statisticId; }
};

class StatisticBuildLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    StatisticBuildLogicalOperator(
        WeakLogicalOperator self,
        std::vector<StatisticAggregation> statisticAggregations,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic,
        LogicalStatisticFields logicalStatisticFields);

    StatisticBuildLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        std::vector<StatisticAggregation> statisticAggregations,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic,
        LogicalStatisticFields logicalStatisticFields);

    static TypedLogicalOperator<StatisticBuildLogicalOperator> create(
        std::vector<StatisticAggregation> statisticAggregations,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic,
        LogicalStatisticFields logicalStatisticFields = {});

    static TypedLogicalOperator<StatisticBuildLogicalOperator> create(
        LogicalOperator child,
        std::vector<StatisticAggregation> statisticAggregations,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic,
        LogicalStatisticFields logicalStatisticFields = {});

    [[nodiscard]] const std::vector<StatisticAggregation>& getStatisticAggregations() const;
    [[nodiscard]] std::vector<WindowAggregationLogicalFunction> getWindowAggregation() const;
    [[nodiscard]] Windowing::TimeBasedWindowType getWindowType() const;
    [[nodiscard]] const LogicalStatisticFields& getLogicalStatisticFields() const;

    [[nodiscard]] const UnqualifiedUnboundField& getWindowStartField() const;
    [[nodiscard]] const UnqualifiedUnboundField& getWindowEndField() const;
    [[nodiscard]] const UnqualifiedUnboundField& getNumberOfSeenMeasurementsField() const;
    [[nodiscard]] std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic> getCharacteristic() const;

    [[nodiscard]] bool operator==(const StatisticBuildLogicalOperator& rhs) const;

    [[nodiscard]] StatisticBuildLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] StatisticBuildLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] StatisticBuildLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] StatisticBuildLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "StatisticBuild";

    std::optional<LogicalOperator> child;
    Windowing::TimeBasedWindowType windowType;
    std::vector<StatisticAggregation> statisticAggregations;
    Windowing::TimeCharacteristic timestampField;
    LogicalStatisticFields logicalStatisticFields;

    void inferLocalSchema();
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;

    friend struct std::hash<StatisticBuildLogicalOperator>;
};

template <>
struct Reflector<TypedLogicalOperator<StatisticBuildLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<StatisticBuildLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<StatisticBuildLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<StatisticBuildLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<StatisticBuildLogicalOperator>);

}

template <>
struct std::hash<NES::StatisticAggregation>
{
    std::size_t operator()(const NES::StatisticAggregation& agg) const noexcept
    {
        return folly::hash::hash_combine(
            std::hash<NES::WindowAggregationLogicalFunction>{}(agg.function), std::hash<NES::StatisticId>{}(agg.statisticId));
    }
};

template <>
struct std::hash<NES::StatisticBuildLogicalOperator>
{
    std::size_t operator()(const NES::StatisticBuildLogicalOperator& op) const noexcept;
};

namespace NES::detail
{
struct ReflectedStatisticBuildLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    Windowing::TimeBasedWindowType windowType;
    std::vector<StatisticAggregation> statisticAggregations;
    Windowing::TimeCharacteristic timestampField;
};
}
