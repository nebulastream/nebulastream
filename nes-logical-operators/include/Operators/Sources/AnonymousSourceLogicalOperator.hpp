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
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Field.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// AnonymousSourceLogicalOperator objects represent physical sources in the logical query plan that are defined within a query as opposed to
/// sources defined in separate create statements. The AnonymousSourceLogicalOperator objects contain all necessary configurations to
/// build a SourceDescriptorLogicalOperator within the AnonymousSourceBindingPhase of the optimizer.

class AnonymousSourceLogicalOperator : public ManagedByOperator
{
public:
    explicit AnonymousSourceLogicalOperator(
        WeakLogicalOperator self,
        Identifier type,
        Schema<UnqualifiedUnboundField, Ordered> sourceSchema,
        std::unordered_map<Identifier, std::string> sourceConfig,
        std::unordered_map<Identifier, std::string> parserConfig);

    static TypedLogicalOperator<AnonymousSourceLogicalOperator> create(
        Identifier type,
        Schema<UnqualifiedUnboundField, Ordered> sourceSchema,
        std::unordered_map<Identifier, std::string> sourceConfig,
        std::unordered_map<Identifier, std::string> parserConfig);

    [[nodiscard]] bool operator==(const AnonymousSourceLogicalOperator& rhs) const;

    [[nodiscard]] AnonymousSourceLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] AnonymousSourceLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] AnonymousSourceLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] static Schema<Field, Unordered> getOutputSchema();

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] static std::string_view getName() noexcept;

    [[nodiscard]] static AnonymousSourceLogicalOperator withInferredSchema();

    [[nodiscard]] Identifier getSourceType() const;
    [[nodiscard]] std::unordered_map<Identifier, std::string> getSourceConfig() const;
    [[nodiscard]] std::unordered_map<Identifier, std::string> getParserConfig() const;
    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getSourceSchema() const;

private:
    static constexpr std::string_view NAME = "AnonymousSource";

    Schema<UnqualifiedUnboundField, Ordered> sourceSchema;
    Identifier sourceType;
    std::unordered_map<Identifier, std::string> sourceConfig;
    std::unordered_map<Identifier, std::string> parserConfig;

    std::vector<LogicalOperator> children;

    TraitSet traitSet;

    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    friend Reflector<TypedLogicalOperator<AnonymousSourceLogicalOperator>>;
};

template <>
struct Reflector<TypedLogicalOperator<AnonymousSourceLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<AnonymousSourceLogicalOperator>&, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<AnonymousSourceLogicalOperator>>
{
    TypedLogicalOperator<AnonymousSourceLogicalOperator> operator()(const Reflected&, const ReflectionContext&) const;
};

static_assert(LogicalOperatorConcept<AnonymousSourceLogicalOperator>);

}

template <>
struct std::hash<NES::AnonymousSourceLogicalOperator>
{
    uint64_t operator()(const NES::AnonymousSourceLogicalOperator& op) const noexcept;
};
