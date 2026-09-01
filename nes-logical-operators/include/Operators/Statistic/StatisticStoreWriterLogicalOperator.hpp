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
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Statistics/Statistic.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Writes the statistic blob produced by an upstream StatisticBuildLogicalOperator into the worker's statistic store.
/// Expects the child schema to contain the four statistic build fields and re-emits per input record:
/// [statisticId: UINT64, statisticStart: UINT64, statisticEnd: UINT64, statisticNumberOfSeenTuples: UINT64]
/// (i.e., the blob is dropped from the stream after it has been stored).
class StatisticStoreWriterLogicalOperator final : public ManagedByOperator
{
public:
    StatisticStoreWriterLogicalOperator(WeakLogicalOperator self, StatisticId statisticId, StatisticType statisticType);
    StatisticStoreWriterLogicalOperator(
        WeakLogicalOperator self, LogicalOperator child, StatisticId statisticId, StatisticType statisticType);

    static TypedLogicalOperator<StatisticStoreWriterLogicalOperator> create(StatisticId statisticId, StatisticType statisticType);
    static TypedLogicalOperator<StatisticStoreWriterLogicalOperator>
    create(LogicalOperator child, StatisticId statisticId, StatisticType statisticType);

    [[nodiscard]] StatisticId getStatisticId() const;
    [[nodiscard]] StatisticType getStatisticType() const;

    [[nodiscard]] bool operator==(const StatisticStoreWriterLogicalOperator& rhs) const;

    [[nodiscard]] StatisticStoreWriterLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] StatisticStoreWriterLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] StatisticStoreWriterLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] StatisticStoreWriterLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "StatisticStoreWriter";

    StatisticId statisticId;
    StatisticType statisticType;

    std::optional<LogicalOperator> child;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;

    friend struct std::hash<StatisticStoreWriterLogicalOperator>;
    friend Reflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>;
};

namespace detail
{
struct ReflectedStatisticStoreWriterLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    StatisticId statisticId{StatisticId::INVALID};
    StatisticType statisticType{};
};
}

template <>
struct Reflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<StatisticStoreWriterLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<StatisticStoreWriterLogicalOperator>
    operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<StatisticStoreWriterLogicalOperator>);

}

template <>
struct std::hash<NES::StatisticStoreWriterLogicalOperator>
{
    std::size_t operator()(const NES::StatisticStoreWriterLogicalOperator& statisticStoreWriterLogicalOperator) const noexcept;
};
