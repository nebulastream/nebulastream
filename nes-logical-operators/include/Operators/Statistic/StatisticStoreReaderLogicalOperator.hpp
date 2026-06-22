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

/// Lean PoC operator: reads a scalar statistic value back from the shared StatisticStore.
/// Schema passthrough — its input record carries (statisticId, start, end) plus a placeholder value field
/// (supplied upstream by a projection of constants); at runtime the physical operator looks the value up in
/// the store and overwrites the value field, forwarding the record onward (e.g. to a FileSink → FIFO).
class StatisticStoreReaderLogicalOperator : public ManagedByOperator
{
public:
    StatisticStoreReaderLogicalOperator(
        WeakLogicalOperator self,
        std::string statisticIdFieldName,
        std::string startFieldName,
        std::string endFieldName,
        std::string valueFieldName);
    StatisticStoreReaderLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        std::string statisticIdFieldName,
        std::string startFieldName,
        std::string endFieldName,
        std::string valueFieldName);

    static TypedLogicalOperator<StatisticStoreReaderLogicalOperator>
    create(std::string statisticIdFieldName, std::string startFieldName, std::string endFieldName, std::string valueFieldName);
    static TypedLogicalOperator<StatisticStoreReaderLogicalOperator> create(
        LogicalOperator child,
        std::string statisticIdFieldName,
        std::string startFieldName,
        std::string endFieldName,
        std::string valueFieldName);

    [[nodiscard]] const std::string& getStatisticIdFieldName() const;
    [[nodiscard]] const std::string& getStartFieldName() const;
    [[nodiscard]] const std::string& getEndFieldName() const;
    [[nodiscard]] const std::string& getValueFieldName() const;

    [[nodiscard]] bool operator==(const StatisticStoreReaderLogicalOperator& rhs) const;

    [[nodiscard]] StatisticStoreReaderLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] StatisticStoreReaderLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] StatisticStoreReaderLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] StatisticStoreReaderLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "StatisticStoreReader";
    std::optional<LogicalOperator> child;
    std::string statisticIdFieldName;
    std::string startFieldName;
    std::string endFieldName;
    std::string valueFieldName;

    void inferLocalSchema();
    /// Set during schema inference (passthrough of the child schema).
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<StatisticStoreReaderLogicalOperator>;
};

namespace detail
{
struct ReflectedStatisticStoreReaderLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::string statisticIdFieldName;
    std::string startFieldName;
    std::string endFieldName;
    std::string valueFieldName;
};
}

template <>
struct Reflector<TypedLogicalOperator<StatisticStoreReaderLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<StatisticStoreReaderLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<StatisticStoreReaderLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<StatisticStoreReaderLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<StatisticStoreReaderLogicalOperator>);
}

template <>
struct std::hash<NES::StatisticStoreReaderLogicalOperator>
{
    uint64_t operator()(const NES::StatisticStoreReaderLogicalOperator& op) const noexcept;
};
