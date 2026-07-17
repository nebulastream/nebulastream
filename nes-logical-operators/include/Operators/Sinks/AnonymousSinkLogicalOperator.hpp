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

/// AnonymousSinkLogicalOperator objects represent sinks in the logical query plan that are defined within a query as opposed to
/// sinks defined in separate create statements. The AnonymousSinkLogicalOperator objects contain all necessary configurations to
/// build a SinkLogicalOperator within the AnonymousSinkBindingPhase of the optimizer.
class AnonymousSinkLogicalOperator : public ManagedByOperator
{
public:
    explicit AnonymousSinkLogicalOperator(
        WeakLogicalOperator self,
        Identifier sinkType,
        std::optional<Schema<UnqualifiedUnboundField, Ordered>> schema,
        std::unordered_map<Identifier, std::string> config,
        std::unordered_map<Identifier, std::string> formatConfig);

    static TypedLogicalOperator<AnonymousSinkLogicalOperator> create(
        Identifier sinkType,
        std::optional<Schema<UnqualifiedUnboundField, Ordered>> schema,
        std::unordered_map<Identifier, std::string> config,
        std::unordered_map<Identifier, std::string> formatConfig);

    [[nodiscard]] bool operator==(const AnonymousSinkLogicalOperator& rhs) const;

    [[nodiscard]] AnonymousSinkLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] AnonymousSinkLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] AnonymousSinkLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] static Schema<Field, Unordered> getOutputSchema();

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] static std::string_view getName() noexcept;

    [[nodiscard]] static AnonymousSinkLogicalOperator withInferredSchema();

    [[nodiscard]] Identifier getSinkType() const;
    [[nodiscard]] std::unordered_map<Identifier, std::string> getSinkConfig() const;
    [[nodiscard]] std::optional<Schema<UnqualifiedUnboundField, Ordered>> getTargetSchema() const;
    [[nodiscard]] std::unordered_map<Identifier, std::string> getFormatConfig() const;

private:
    static constexpr std::string_view NAME = "AnonymousSink";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;

    std::optional<Schema<UnqualifiedUnboundField, Ordered>> targetSchema;
    Identifier sinkType;
    std::unordered_map<Identifier, std::string> sinkConfig;
    std::unordered_map<Identifier, std::string> formatConfig;

    friend Reflector<TypedLogicalOperator<AnonymousSinkLogicalOperator>>;
};

template <>
struct Reflector<TypedLogicalOperator<AnonymousSinkLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<AnonymousSinkLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<AnonymousSinkLogicalOperator>>
{
    TypedLogicalOperator<AnonymousSinkLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<AnonymousSinkLogicalOperator>);
}

template <>
struct std::hash<NES::AnonymousSinkLogicalOperator>
{
    uint64_t operator()(const NES::AnonymousSinkLogicalOperator& op) const noexcept;
};
