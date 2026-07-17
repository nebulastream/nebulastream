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
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Configurations/ConfigResolution.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include "InputFormatterDescriptor.hpp"
#include "Sources/SourceCatalog.hpp"
#include "Sources/SourceDescriptor.hpp"


namespace NES
{

/// InlineSourceLogicalOperator objects represent physical sources in the logical query plan that are defined within a query as opposed to
/// sources defined in separate create statements. The InlineSourceLogicalOperator objects contain all necessary configurations to
/// build a SourceDescriptorLogicalOperator within the InlineSourceBindingPhase of the optimizer.

class InlineSourceLogicalOperator : public ManagedByOperator
{
public:
    explicit InlineSourceLogicalOperator(
        WeakLogicalOperator self,
        Schema<UnqualifiedUnboundField, Ordered> sourceSchema,
        GeneralSourceConfig generalSourceConfig,
        PluginSourceConfiguration pluginSourceConfig,
        InputFormatterDescriptor inputFormatterDescriptor);

    static TypedLogicalOperator<InlineSourceLogicalOperator> create(
        Schema<UnqualifiedUnboundField, Ordered> sourceSchema,
        GeneralSourceConfig generalSourceConfig,
        PluginSourceConfiguration pluginSourceConfig,
        InputFormatterDescriptor inputFormatterDescriptor);

    [[nodiscard]] bool operator==(const InlineSourceLogicalOperator& rhs) const;

    [[nodiscard]] InlineSourceLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InlineSourceLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] InlineSourceLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] static Schema<Field, Unordered> getOutputSchema();

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] static std::string_view getName() noexcept;

    [[nodiscard]] static InlineSourceLogicalOperator withInferredSchema();

    /// The literals exactly as the parser produced them (see getSourceConfigLiterals); resolved
    /// against the source's declared config schema in the InlineSourceBindingRule.
    [[nodiscard]] const Schema<UnqualifiedUnboundField, Ordered>& getSourceSchema() const;

    [[nodiscard]] const GeneralSourceConfig& getGeneralSourceConfig() const;
    [[nodiscard]] const PluginSourceConfiguration& getPluginSourceConfig() const;
    [[nodiscard]] const InputFormatterDescriptor& getInputFormatterDescriptor() const;

private:
    static constexpr std::string_view NAME = "InlineSource";

    Schema<UnqualifiedUnboundField, Ordered> sourceSchema;
    GeneralSourceConfig generalSourceConfig;
    PluginSourceConfiguration pluginSourceConfig;
    InputFormatterDescriptor inputFormatterDescriptor;

    std::vector<LogicalOperator> children;

    TraitSet traitSet;

    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    friend Reflector<TypedLogicalOperator<InlineSourceLogicalOperator>>;
};

template <>
struct Reflector<TypedLogicalOperator<InlineSourceLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<InlineSourceLogicalOperator>&) const;
};

template <>
struct Unreflector<TypedLogicalOperator<InlineSourceLogicalOperator>>
{
    TypedLogicalOperator<InlineSourceLogicalOperator> operator()(const Reflected&, const ReflectionContext&) const;
};

static_assert(LogicalOperatorConcept<InlineSourceLogicalOperator>);

}

template <>
struct std::hash<NES::InlineSourceLogicalOperator>
{
    uint64_t operator()(const NES::InlineSourceLogicalOperator& op) const noexcept;
};
