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
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Reads a scalar statistic (Count / Sum / Avg) back out of the statistic store.
///
/// One operator serves all three ops: their payload is the same bare scalar and they differ only in its data type,
/// which the probe has to be told because nothing links it back to the aggregation that produced it. The op is
/// carried too, because the reader dispatches its iterator on the statistic type.
///
/// Only the window bounds come from the incoming record; the statisticId is a member. That is deliberate. It keeps
/// the operator usable in both settings -- chained directly after the build, and driven by impulse tuples from a
/// source -- and it avoids depending on a STATISTICID field surviving a pipeline boundary, which it would not: the
/// writer adds that field to the record but it is not part of any logical output schema, so an Emit between the two
/// would silently drop it.
class ScalarStatisticProbeLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    ScalarStatisticProbeLogicalOperator(
        WeakLogicalOperator self, StatisticId statisticId, StatisticType op, DataType valueType, Identifier startTsFieldName,
        Identifier endTsFieldName);
    ScalarStatisticProbeLogicalOperator(
        WeakLogicalOperator self, LogicalOperator child, StatisticId statisticId, StatisticType op, DataType valueType,
        Identifier startTsFieldName, Identifier endTsFieldName);

    static TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>
    create(StatisticId statisticId, StatisticType op, DataType valueType, Identifier startTsFieldName, Identifier endTsFieldName);
    static TypedLogicalOperator<ScalarStatisticProbeLogicalOperator> create(
        LogicalOperator child, StatisticId statisticId, StatisticType op, DataType valueType, Identifier startTsFieldName,
        Identifier endTsFieldName);

    [[nodiscard]] StatisticId getStatisticId() const { return statisticId; }
    [[nodiscard]] StatisticType getOp() const { return op; }
    [[nodiscard]] DataType getValueType() const { return valueType; }
    [[nodiscard]] Identifier getStartTsFieldName() const { return startTsFieldName; }
    [[nodiscard]] Identifier getEndTsFieldName() const { return endTsFieldName; }

    [[nodiscard]] bool operator==(const ScalarStatisticProbeLogicalOperator& rhs) const;

    [[nodiscard]] ScalarStatisticProbeLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] ScalarStatisticProbeLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] ScalarStatisticProbeLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] ScalarStatisticProbeLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "ScalarStatisticProbe";

    void inferLocalSchema();

    std::optional<LogicalOperator> child;
    StatisticId statisticId;
    StatisticType op;
    DataType valueType;
    Identifier startTsFieldName;
    Identifier endTsFieldName;

    /// Set during schema inference.
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<ScalarStatisticProbeLogicalOperator>;
};

namespace detail
{
/// Strong types are flattened to their underlying representation so no Reflector specialisation is needed for
/// StatisticId or StatisticType.
struct ReflectedScalarStatisticProbeLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    uint64_t statisticId{};
    uint64_t op{};
    DataType valueType;
    Identifier startTsFieldName;
    Identifier endTsFieldName;
};
}

template <>
struct Reflector<TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<ScalarStatisticProbeLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<ScalarStatisticProbeLogicalOperator>);

}

template <>
struct std::hash<NES::ScalarStatisticProbeLogicalOperator>
{
    size_t operator()(const NES::ScalarStatisticProbeLogicalOperator& op) const noexcept;
};
