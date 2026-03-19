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
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <DataTypes/LegacySchema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

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
        std::string type,
        const LegacySchema& schema,
        std::unordered_map<std::string, std::string> sourceConfig,
        std::unordered_map<std::string, std::string> parserConfig);

    [[nodiscard]] bool operator==(const InlineSourceLogicalOperator& rhs) const;

    [[nodiscard]] InlineSourceLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InlineSourceLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<LegacySchema> getInputSchemas() const;
    [[nodiscard]] LegacySchema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] static std::string_view getName() noexcept;

    [[nodiscard]] InlineSourceLogicalOperator withInferredSchema(const std::vector<LegacySchema>& inputSchemas) const;

    [[nodiscard]] std::string getSourceType() const;
    [[nodiscard]] std::unordered_map<std::string, std::string> getSourceConfig() const;
    [[nodiscard]] std::unordered_map<std::string, std::string> getParserConfig() const;
    [[nodiscard]] LegacySchema getSchema() const;

private:
    static constexpr std::string_view NAME = "InlineSource";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;

    LegacySchema schema;

    std::string sourceType;
    std::unordered_map<std::string, std::string> sourceConfig;
    std::unordered_map<std::string, std::string> parserConfig;

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
    TypedLogicalOperator<InlineSourceLogicalOperator> operator()(const Reflected&) const;
};

static_assert(LogicalOperatorConcept<InlineSourceLogicalOperator>);

}
