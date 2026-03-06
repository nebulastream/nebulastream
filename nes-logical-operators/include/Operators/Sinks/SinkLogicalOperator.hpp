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

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Schema/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

struct SinkLogicalOperator final
{
    /// During query parsing, we require the name of the sink and need to assign it an id.
    explicit SinkLogicalOperator(WeakLogicalOperator self, Identifier sinkName);
    explicit SinkLogicalOperator(WeakLogicalOperator self, const SinkDescriptor& sinkDescriptor);

    [[nodiscard]] bool operator==(const SinkLogicalOperator& rhs) const;

    [[nodiscard]] SinkLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] SinkLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] SinkLogicalOperator withInferredSchema() const;

    [[nodiscard]] Identifier getSinkName() const noexcept;
    [[nodiscard]] std::optional<SinkDescriptor> getSinkDescriptor() const;

    [[nodiscard]] SinkLogicalOperator withSinkDescriptor(SinkDescriptor sinkDescriptor) const;

    WeakLogicalOperator self;

private:
    static constexpr std::string_view NAME = "Sink";

    TraitSet traitSet;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;

    Identifier sinkName;
    std::optional<SinkDescriptor> sinkDescriptor;
    std::optional<LogicalOperator> child;

    void inferLocalSchema();

    friend class OperatorSerializationUtil;
    friend struct std::hash<SinkLogicalOperator>;
};

template <>
struct Reflector<TypedLogicalOperator<SinkLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<SinkLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<SinkLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<SinkLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<SinkLogicalOperator>);
}

namespace NES::detail
{
struct ReflectedSinkLogicalOperator
{
    OperatorId operatorId;
    std::optional<SinkDescriptor> sinkDescriptor;
    Identifier sinkName;
};
}

template <>
struct std::hash<NES::SinkLogicalOperator>
{
    size_t operator()(const NES::SinkLogicalOperator& sinkLogicalOperator) const noexcept;
};
