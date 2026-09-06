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

#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Operators/Statistic/StatisticWindowMatch.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Reads a statistic back out of the statistic store, whatever its payload layout.
///
/// The payload is opaque, so the probe is told how to decode it: `blobName` selects the decoder and `payloadFields`
/// names the columns it produces -- one field for a scalar aggregation, one per sampled column for a reservoir
/// sample. The name is also what lets a probe reject a statistic some other build wrote.
///
/// Only the window bounds come from the incoming record; the statisticId is a member. That is deliberate. It keeps
/// the operator usable in both settings -- chained directly after the build, and driven by impulse tuples from a
/// source -- and it avoids depending on a STATISTICID field surviving a pipeline boundary, which it would not: the
/// writer adds that field to the record but it is not part of any logical output schema, so an Emit between the two
/// would silently drop it.
class StatisticStoreReaderLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    /// One decoded column of the payload: a scalar contributes exactly one, a reservoir sample one per sampled field.
    using PayloadField = std::pair<Identifier, DataType>;

    StatisticStoreReaderLogicalOperator(
        WeakLogicalOperator self,
        StatisticId statisticId,
        StatisticBlobType typeName,
        std::vector<PayloadField> payloadFields,
        StatisticWindowMatch windowMatch);
    StatisticStoreReaderLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        StatisticId statisticId,
        StatisticBlobType typeName,
        std::vector<PayloadField> payloadFields,
        StatisticWindowMatch windowMatch);

    static TypedLogicalOperator<StatisticStoreReaderLogicalOperator>
    create(StatisticId statisticId, StatisticBlobType typeName, std::vector<PayloadField> payloadFields, StatisticWindowMatch windowMatch);
    static TypedLogicalOperator<StatisticStoreReaderLogicalOperator> create(
        LogicalOperator child,
        StatisticId statisticId,
        StatisticBlobType typeName,
        std::vector<PayloadField> payloadFields,
        StatisticWindowMatch windowMatch);

    [[nodiscard]] StatisticId getStatisticId() const { return statisticId; }

    [[nodiscard]] const StatisticBlobType& getTypeName() const { return typeName; }

    [[nodiscard]] const std::vector<PayloadField>& getPayloadFields() const { return payloadFields; }

    [[nodiscard]] StatisticWindowMatch getWindowMatch() const { return windowMatch; }

    [[nodiscard]] bool operator==(const StatisticStoreReaderLogicalOperator& rhs) const;

    [[nodiscard]] StatisticStoreReaderLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] StatisticStoreReaderLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] StatisticStoreReaderLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] StatisticStoreReaderLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "StatisticStoreReader";

    void inferLocalSchema();

    std::optional<LogicalOperator> child;
    StatisticId statisticId;
    StatisticBlobType typeName;
    std::vector<PayloadField> payloadFields;
    StatisticWindowMatch windowMatch;

    /// Set during schema inference.
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<StatisticStoreReaderLogicalOperator>;
};

namespace detail
{
/// Strong types are flattened to their underlying representation so no Reflector specialisation is needed for
/// StatisticId.
struct ReflectedStatisticStoreReaderLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    uint64_t statisticId{};
    std::string typeName;
    std::vector<StatisticStoreReaderLogicalOperator::PayloadField> payloadFields;
    uint8_t windowMatch{};
};
}

template <>
struct Reflector<TypedLogicalOperator<StatisticStoreReaderLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<StatisticStoreReaderLogicalOperator>& op, const ReflectionContext& context) const;
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
    size_t operator()(const NES::StatisticStoreReaderLogicalOperator& op) const noexcept;
};
