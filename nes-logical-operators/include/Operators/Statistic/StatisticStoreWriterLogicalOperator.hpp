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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Lean PoC operator: writes a scalar statistic value into the shared StatisticStore.
/// It is a schema passthrough — it reads the window start/end and a value field from its child's
/// output and inserts (statisticId, start, end, value) into the store, forwarding the record onward.
/// The statisticId is a constant assigned by the StatisticCoordinator (kept as a plain uint64 here so
/// nes-logical-operators does not depend on nes-statistics, which depends on this module).
class StatisticStoreWriterLogicalOperator : public ManagedByOperator
{
public:
    StatisticStoreWriterLogicalOperator(
        WeakLogicalOperator self, uint64_t statisticId, std::string startFieldName, std::string endFieldName, std::string valueFieldName);
    StatisticStoreWriterLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        uint64_t statisticId,
        std::string startFieldName,
        std::string endFieldName,
        std::string valueFieldName);

    static TypedLogicalOperator<StatisticStoreWriterLogicalOperator>
    create(uint64_t statisticId, std::string startFieldName, std::string endFieldName, std::string valueFieldName);
    static TypedLogicalOperator<StatisticStoreWriterLogicalOperator>
    create(LogicalOperator child, uint64_t statisticId, std::string startFieldName, std::string endFieldName, std::string valueFieldName);

    [[nodiscard]] uint64_t getStatisticId() const;
    [[nodiscard]] const std::string& getStartFieldName() const;
    [[nodiscard]] const std::string& getEndFieldName() const;
    [[nodiscard]] const std::string& getValueFieldName() const;

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
    std::optional<LogicalOperator> child;
    uint64_t statisticId;
    std::string startFieldName;
    std::string endFieldName;
    std::string valueFieldName;

    void inferLocalSchema();
    /// Set during schema inference (passthrough of the child schema).
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<StatisticStoreWriterLogicalOperator>;
};

namespace detail
{
struct ReflectedStatisticStoreWriterLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    uint64_t statisticId{};
    std::string startFieldName;
    std::string endFieldName;
    std::string valueFieldName;
};
}

template <>
struct Reflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<StatisticStoreWriterLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<StatisticStoreWriterLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<StatisticStoreWriterLogicalOperator>);
}

template <>
struct std::hash<NES::StatisticStoreWriterLogicalOperator>
{
    uint64_t operator()(const NES::StatisticStoreWriterLogicalOperator& op) const noexcept;
};
