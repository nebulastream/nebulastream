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
#include <Schema/Schema.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

/// InlineSourceLogicalOperator objects represent physical sources in the logical query plan that are defined within a query as opposed to
/// sources defined in separate create statements. The InlineSourceLogicalOperator objects contain all necessary configurations to
/// build a SourceDescriptorLogicalOperator within the InlineSourceBindingPhase of the optimizer.

class InlineSourceLogicalOperator
{
public:
    explicit InlineSourceLogicalOperator(
        Identifier type,
        SchemaBase<UnboundFieldBase<1>, true> sourceSchema,
        std::unordered_map<Identifier, std::string> sourceConfig,
        std::unordered_map<Identifier, std::string> parserConfig);

    [[nodiscard]] bool operator==(const InlineSourceLogicalOperator& rhs) const;
    static void serialize(SerializableOperator&);

    [[nodiscard]] InlineSourceLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InlineSourceLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] static std::string_view getName() noexcept;

    [[nodiscard]] InlineSourceLogicalOperator withInferredSchema() const;

    [[nodiscard]] Identifier getSourceType() const;
    [[nodiscard]] std::unordered_map<Identifier, std::string> getSourceConfig() const;
    [[nodiscard]] std::unordered_map<Identifier, std::string> getParserConfig() const;
    [[nodiscard]] SchemaBase<UnboundFieldBase<1>, true> getSourceSchema() const;

public:
    WeakLogicalOperator self;

private:
    static constexpr std::string_view NAME = "InlineSource";

    SchemaBase<UnboundFieldBase<1>, true> sourceSchema;
    Identifier sourceType;
    std::unordered_map<Identifier, std::string> sourceConfig;
    std::unordered_map<Identifier, std::string> parserConfig;

    std::vector<LogicalOperator> children;

    TraitSet traitSet;

    /// Set during schema inference
    std::optional<SchemaBase<UnboundFieldBase<1>, false>> outputSchema;

};

static_assert(LogicalOperatorConcept<InlineSourceLogicalOperator>);

}

template <>
struct std::hash<NES::InlineSourceLogicalOperator>
{
    uint64_t operator()(const NES::InlineSourceLogicalOperator& op) const noexcept;
};
