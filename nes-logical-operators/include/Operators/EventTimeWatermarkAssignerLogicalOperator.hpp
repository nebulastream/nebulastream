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
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Serialization/LogicalOperatorReflection.hpp>

namespace NES
{

class EventTimeWatermarkAssignerLogicalOperator
{
public:
    EventTimeWatermarkAssignerLogicalOperator(WeakLogicalOperator self, LogicalFunction onField, const Windowing::TimeUnit& unit);
    EventTimeWatermarkAssignerLogicalOperator(WeakLogicalOperator self, LogicalOperator child, LogicalFunction onField, const Windowing::TimeUnit& unit);


    [[nodiscard]] bool operator==(const EventTimeWatermarkAssignerLogicalOperator& rhs) const;

    [[nodiscard]] EventTimeWatermarkAssignerLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] EventTimeWatermarkAssignerLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] EventTimeWatermarkAssignerLogicalOperator withInferredSchema() const;
    [[nodiscard]] LogicalFunction getOnField() const;
    [[nodiscard]] Windowing::TimeUnit getUnit() const;

    WeakLogicalOperator self;
private:
    static constexpr std::string_view NAME = "EventTimeWatermarkAssigner";

    Windowing::TimeUnit unit;
    LogicalFunction onField;

    /// TOOD make non-optional once all ctors require children
    std::optional<LogicalOperator> child;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;

    friend struct std::hash<EventTimeWatermarkAssignerLogicalOperator>;
};

namespace detail
{
struct ReflectedEventTimeWatermarkAssignerLogicalOperator
{
    OperatorId operatorId;
    LogicalFunction onField;
    Windowing::TimeUnit timeUnit;
};
}
template <>
struct Reflector<TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<EventTimeWatermarkAssignerLogicalOperator>);
}

template <>
struct std::hash<NES::EventTimeWatermarkAssignerLogicalOperator>
{
    uint64_t operator()(const NES::EventTimeWatermarkAssignerLogicalOperator& eventTimeWatermarkAssignerOperator) const noexcept;
};
