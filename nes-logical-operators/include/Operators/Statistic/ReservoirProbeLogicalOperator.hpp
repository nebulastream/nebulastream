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
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
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

/// Unpacks reservoir samples from the worker's statistic store back into a record stream.
/// For every input record (the metadata emitted by a StatisticStoreWriterLogicalOperator), it looks up the sample
/// blob for (statisticId, statisticStart, statisticEnd) in the store and emits one record per sampled tuple.
/// As the sample blob is opaque, the decode schema is declared on the operator (sampleFields).
/// Output schema: [statisticStart: UINT64, statisticEnd: UINT64, statisticNumberOfSeenTuples: UINT64] ++ sampleFields.
class ReservoirProbeLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    using SampleField = std::pair<Identifier, DataType>;

    ReservoirProbeLogicalOperator(WeakLogicalOperator self, StatisticId statisticId, std::vector<SampleField> sampleFields);
    ReservoirProbeLogicalOperator(
        WeakLogicalOperator self, LogicalOperator child, StatisticId statisticId, std::vector<SampleField> sampleFields);

    static TypedLogicalOperator<ReservoirProbeLogicalOperator> create(StatisticId statisticId, std::vector<SampleField> sampleFields);
    static TypedLogicalOperator<ReservoirProbeLogicalOperator>
    create(LogicalOperator child, StatisticId statisticId, std::vector<SampleField> sampleFields);

    [[nodiscard]] StatisticId getStatisticId() const;
    [[nodiscard]] const std::vector<SampleField>& getSampleFields() const;

    [[nodiscard]] bool operator==(const ReservoirProbeLogicalOperator& rhs) const;

    [[nodiscard]] ReservoirProbeLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] ReservoirProbeLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] ReservoirProbeLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] ReservoirProbeLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "ReservoirProbe";

    StatisticId statisticId;
    std::vector<SampleField> sampleFields;

    std::optional<LogicalOperator> child;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;

    friend struct std::hash<ReservoirProbeLogicalOperator>;
    friend Reflector<TypedLogicalOperator<ReservoirProbeLogicalOperator>>;
};

namespace detail
{
struct ReflectedReservoirProbeLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    StatisticId statisticId{StatisticId::INVALID};
    std::vector<ReservoirProbeLogicalOperator::SampleField> sampleFields;
};
}

template <>
struct Reflector<TypedLogicalOperator<ReservoirProbeLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<ReservoirProbeLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<ReservoirProbeLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<ReservoirProbeLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<ReservoirProbeLogicalOperator>);

}

template <>
struct std::hash<NES::ReservoirProbeLogicalOperator>
{
    std::size_t operator()(const NES::ReservoirProbeLogicalOperator& reservoirProbeLogicalOperator) const noexcept;
};
