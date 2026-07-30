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
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Schema/Field.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>

namespace NES
{

class SampleLogicalOperator : public OriginIdAssigner, public ManagedByOperator
{
public:
    /// nullopt means: do not bound the tick source: SampleTickSource's own MAX_RUNTIME_MS/MAX_TICKS config
    /// parameters already default to running indefinitely, so an unset value here is simply left unset there.
    SampleLogicalOperator(
        WeakLogicalOperator self,
        Windowing::TimeCharacteristic timestampField,
        uint64_t slotDurationMs,
        std::optional<std::string> strategy,
        std::optional<int32_t> maxRuntimeMs = std::nullopt,
        std::optional<int64_t> maxTicks = std::nullopt);
    SampleLogicalOperator(
        WeakLogicalOperator self,
        std::array<LogicalOperator, 2> children,
        Windowing::TimeCharacteristic timestampField,
        uint64_t slotDurationMs,
        std::optional<std::string> strategy,
        std::optional<int32_t> maxRuntimeMs = std::nullopt,
        std::optional<int64_t> maxTicks = std::nullopt);

    static TypedLogicalOperator<SampleLogicalOperator> create(
        Windowing::TimeCharacteristic timestampField,
        uint64_t slotDurationMs,
        std::optional<std::string> strategy,
        std::optional<int32_t> maxRuntimeMs = std::nullopt,
        std::optional<int64_t> maxTicks = std::nullopt);
    static TypedLogicalOperator<SampleLogicalOperator> create(
        std::array<LogicalOperator, 2> children,
        Windowing::TimeCharacteristic timestampField,
        uint64_t slotDurationMs,
        std::optional<std::string> strategy,
        std::optional<int32_t> maxRuntimeMs = std::nullopt,
        std::optional<int64_t> maxTicks = std::nullopt);

    [[nodiscard]] bool operator==(const SampleLogicalOperator& rhs) const;

    [[nodiscard]] SampleLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] SampleLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] SampleLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] SampleLogicalOperator withInferredSchema() const;

    [[nodiscard]] Windowing::TimeCharacteristic getCharacteristic() const;
    [[nodiscard]] uint64_t getSlotDurationMs() const;
    [[nodiscard]] std::string getStrategy() const;
    [[nodiscard]] std::optional<int32_t> getMaxRuntimeMs() const;
    [[nodiscard]] std::optional<int64_t> getMaxTicks() const;

private:
    static constexpr std::string_view NAME = "Sample";
    static constexpr std::string_view DEFAULT_STRATEGY = "LAST";

    Windowing::TimeCharacteristic timestampField;
    uint64_t slotDurationMs;
    std::string strategy;
    std::optional<int32_t> maxRuntimeMs;
    std::optional<int64_t> maxTicks;
    std::vector<LogicalOperator> children;
    TraitSet traitSet;

    void inferLocalSchema();
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    friend Reflector<TypedLogicalOperator<SampleLogicalOperator>>;
    friend struct std::hash<SampleLogicalOperator>;
};

template <>
struct Reflector<TypedLogicalOperator<SampleLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<SampleLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<SampleLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<SampleLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<SampleLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedSampleLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    Windowing::TimeCharacteristic timestampField;
    uint64_t slotDurationMs;
    std::string strategy;
    std::optional<int32_t> maxRuntimeMs;
    std::optional<int64_t> maxTicks;
};
}

template <>
struct std::hash<NES::SampleLogicalOperator>
{
    std::size_t operator()(const NES::SampleLogicalOperator& sampleLogicalOperator) const noexcept;
};
