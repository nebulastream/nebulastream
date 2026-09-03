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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Reorderer.hpp>
#include <Operators/Reprojecter.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Statistics/Statistic.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>

namespace NES
{

/// Builds a statistic (currently only a reservoir sample) over the tuples of a time-based window.
/// Structurally a sibling of WindowedAggregationLogicalOperator: it aggregates all tuples of a window, but into a
/// single statistic blob instead of per-key aggregates, and its output schema is fixed:
/// [statisticStart: UINT64, statisticEnd: UINT64, statisticData: VARSIZED, statisticNumberOfSeenTuples: UINT64].
/// The statistic blob is written into the worker's statistic store by a downstream StatisticStoreWriterLogicalOperator.
class StatisticBuildLogicalOperator final : public OriginIdAssigner, public Reprojecter, public Reorderer, public ManagedByOperator
{
public:
    StatisticBuildLogicalOperator(
        WeakLogicalOperator self,
        StatisticId statisticId,
        uint64_t sampleSize,
        uint64_t seed,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic);

    StatisticBuildLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        StatisticId statisticId,
        uint64_t sampleSize,
        uint64_t seed,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic);

    static TypedLogicalOperator<StatisticBuildLogicalOperator> create(
        StatisticId statisticId,
        uint64_t sampleSize,
        uint64_t seed,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic);

    static TypedLogicalOperator<StatisticBuildLogicalOperator> create(
        LogicalOperator child,
        StatisticId statisticId,
        uint64_t sampleSize,
        uint64_t seed,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic);

    [[nodiscard]] StatisticId getStatisticId() const;
    [[nodiscard]] uint64_t getSampleSize() const;
    [[nodiscard]] uint64_t getSeed() const;
    [[nodiscard]] Windowing::TimeBasedWindowType getWindowType() const;
    [[nodiscard]] std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic> getCharacteristic() const;

    [[nodiscard]] const UnqualifiedUnboundField& getStatisticStartField() const;
    [[nodiscard]] const UnqualifiedUnboundField& getStatisticEndField() const;
    [[nodiscard]] const UnqualifiedUnboundField& getStatisticDataField() const;
    [[nodiscard]] const UnqualifiedUnboundField& getNumberOfSeenTuplesField() const;

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
    [[nodiscard]] std::unordered_map<Field, std::unordered_set<Field>> getAccessedFieldsForOutput() const override;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;
    [[nodiscard]] const DynamicBase* getDynamicBase() const;

private:
    static constexpr std::string_view NAME = "StatisticBuild";

    std::optional<LogicalOperator> child;
    Windowing::TimeBasedWindowType windowType;
    StatisticId statisticId;
    uint64_t sampleSize;
    uint64_t seed;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
    Windowing::TimeCharacteristic timestampField;

    /// Fixed output fields: statisticStart, statisticEnd, statisticData, statisticNumberOfSeenTuples
    std::array<UnqualifiedUnboundField, 4> statisticFields = std::array{
        UnqualifiedUnboundField{Identifier::parse("statisticStart"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("statisticEnd"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("statisticData"), DataType::Type::VARSIZED},
        UnqualifiedUnboundField{Identifier::parse("statisticNumberOfSeenTuples"), DataType::Type::UINT64}};

    TraitSet traitSet;

    friend struct std::hash<StatisticBuildLogicalOperator>;

    friend Reflector<TypedLogicalOperator<StatisticBuildLogicalOperator>>;
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
struct std::hash<NES::StatisticBuildLogicalOperator>
{
    std::size_t operator()(const NES::StatisticBuildLogicalOperator& statisticBuildLogicalOperator) const noexcept;
};

namespace NES::detail
{
struct ReflectedStatisticBuildLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    Windowing::TimeBasedWindowType windowType;
    StatisticId statisticId{StatisticId::INVALID};
    uint64_t sampleSize{};
    uint64_t seed{};
    Windowing::TimeCharacteristic timestampField;
};
}
